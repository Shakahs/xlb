use std::{
    collections::HashMap,
    ffi::OsStr,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, Instant, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use fuser::{
    BackgroundSession, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, Generation,
    INodeNo, LockOwner, MountOption, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request, SessionACL,
};
use futures::{StreamExt, TryStreamExt, stream};
use tokio::runtime::Handle;
use xlb::{AssetClass, BlakeHash, FetchProgress, FetchReport, FetchSources, FetchTier, ProgressSink};
use xlb_core::chunks::{CAS_INDEX_LIMIT, ModelIndex, MODEL_INDEX_PREFIX};

use crate::{chunk_file::ChunkFile, snapshotter::clear_disconnected_mount};

const ENTRY_TTL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct ModelEntries {
    inodes: HashMap<BlakeHash, INodeNo>,
    files: Vec<(BlakeHash, Option<ChunkFile>)>,
}

#[derive(Clone)]
pub struct ModelMount {
    root: PathBuf,
    class: AssetClass,
    entries: Arc<RwLock<ModelEntries>>,
    runtime: Handle,
    concurrency: usize,
}

impl ModelMount {
    pub async fn start(root: &Path, class: AssetClass, concurrency: usize) -> Result<(Self, BackgroundSession)> {
        match tokio::fs::create_dir_all(root).await {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(libc::ENOTCONN) => {}
            Err(error) => return Err(error.into()),
        }
        let mount = Self {
            root: root.to_owned(), class, entries: Arc::default(), runtime: Handle::current(), concurrency,
        };
        let filesystem = mount.clone();
        let session = tokio::task::spawn_blocking(move || {
            let mut config = Config::default();
            config.acl = SessionACL::All;
            config.mount_options = vec![MountOption::RO, MountOption::NoDev, MountOption::NoSuid,
                MountOption::NoExec, MountOption::DefaultPermissions, MountOption::FSName("xlb-models".into())];
            let root = filesystem.root.clone();
            clear_disconnected_mount(&root)?;
            fuser::spawn_mount(filesystem, &root, &config)
                .with_context(|| format!("mount XLB model files at {}", root.display()))
        }).await??;
        Ok((mount, session))
    }

    pub fn path(&self, hash: BlakeHash) -> PathBuf {
        self.root.join(hash.to_hex())
    }

    pub async fn prepare(&self, hash: BlakeHash, report_progress: ProgressSink) -> Result<FetchReport> {
        let started = Instant::now();
        let (ino, file) = self.resolve(hash).await?.context("model has no CAS index")?;
        let total = file.index.size_bytes;
        let file = &file;
        let mut chunks = stream::iter(0..file.index.chunks.len() as u64)
            .map(|ordinal| async move {
                let (_, report) = file.read_chunk(ordinal).await.map_err(|error| self.check_read(ino, file, error))?;
                Ok::<_, anyhow::Error>(report)
            }).buffer_unordered(self.concurrency);
        let mut sources = FetchSources::default();
        let mut bytes = 0;
        let mut peer_bytes = 0;
        while let Some(report) = chunks.try_next().await? {
            bytes += report.bytes_served;
            peer_bytes += report.peer_bytes;
            sources.origin_bytes += report.origin_bytes;
            for (peer, bytes) in report.peers {
                *sources.peers.entry(peer).or_default() += bytes;
            }
            report_progress(FetchProgress {
                bytes_so_far: bytes, total: Some(total), tier: fetch_tier(peer_bytes, sources.origin_bytes), sources: sources.clone(),
            });
        }
        // Asset classes are named once at daemon startup. Use the name the
        // chunk reports use, including for empty model indexes.
        Ok(FetchReport {
            class: self.class.name(), hash,
            tier: Some(fetch_tier(peer_bytes, sources.origin_bytes)), peer_id: None,
            bytes_served: total, peer_bytes, origin_bytes: sources.origin_bytes, peers: sources.peers,
            duration_ms: started.elapsed().as_millis() as u64,
        })
    }

    fn hash(&self, ino: INodeNo) -> Option<BlakeHash> {
        let index = usize::try_from(ino.0.checked_sub(2)?).ok()?;
        self.entries.read().unwrap().files.get(index).map(|(hash, _)| *hash)
    }

    fn check_read(&self, ino: INodeNo, file: &ChunkFile, error: anyhow::Error) -> anyhow::Error {
        if error.downcast_ref::<std::io::Error>().is_some_and(|error| error.kind() == ErrorKind::InvalidData) {
            let mut entries = self.entries.write().unwrap();
            let (_, stored) = &mut entries.files[(ino.0 - 2) as usize];
            if stored.as_ref().is_some_and(|stored| Arc::ptr_eq(&stored.index, &file.index)) {
                *stored = None;
            }
        }
        error
    }

    async fn decode(&self, hash: BlakeHash, bytes: &[u8]) -> Result<ChunkFile> {
        let index: ModelIndex = serde_json::from_slice(bytes).context("parse model CAS index")?;
        ensure!(index.matches_root(hash), "model CAS index does not match its BLAKE3 name");
        let legacy = index.layout.chunks.len() > 1 && index.chaining_values.is_empty();
        let file = ChunkFile::new(Arc::new(index.layout), self.class.clone())?
            .with_model_values(index.chaining_values);
        if legacy {
            let file = &file;
            let mut reads = stream::iter(0..file.index.chunks.len() as u64)
                .map(|ordinal| file.read_chunk(ordinal))
                .buffered(self.concurrency);
            let mut hasher = blake3::Hasher::new();
            while let Some((body, _)) = reads.try_next().await? {
                hasher.update(&body);
            }
            ensure!(hasher.finalize().as_bytes() == hash.as_bytes(),
                "legacy model CAS index does not match its BLAKE3 name");
        }
        Ok(file)
    }

    async fn resolve(&self, hash: BlakeHash) -> Result<Option<(INodeNo, ChunkFile)>> {
        {
            let entries = self.entries.read().unwrap();
            if let Some(&ino) = entries.inodes.get(&hash) {
                if let Some(file) = &entries.files[(ino.0 - 2) as usize].1 {
                    return Ok(Some((ino, file.clone())));
                }
            }
        }
        let key = format!("{MODEL_INDEX_PREFIX}{hash}");
        let Some(bytes) = self.class.read_origin_object(&key, CAS_INDEX_LIMIT).await? else { return Ok(None); };
        let file = self.decode(hash, &bytes).await?;
        let mut entries = self.entries.write().unwrap();
        if let Some(&ino) = entries.inodes.get(&hash) {
            let stored = entries.files[(ino.0 - 2) as usize].1.get_or_insert(file);
            return Ok(Some((ino, stored.clone())));
        }
        let ino = INodeNo(entries.files.len() as u64 + 2);
        entries.inodes.insert(hash, ino);
        entries.files.push((hash, Some(file.clone())));
        Ok(Some((ino, file)))
    }
}

fn fetch_tier(peer_bytes: u64, origin_bytes: u64) -> FetchTier {
    match (peer_bytes, origin_bytes) {
        (0, 0) => FetchTier::Cache,
        (peer, origin) if peer > origin => FetchTier::Peer,
        _ => FetchTier::Origin,
    }
}

fn attr(ino: INodeNo, size: u64) -> FileAttr {
    let root = ino == INodeNo::ROOT;
    FileAttr {
        ino, size, blocks: size.div_ceil(512),
        atime: UNIX_EPOCH, mtime: UNIX_EPOCH, ctime: UNIX_EPOCH, crtime: UNIX_EPOCH,
        kind: if root { FileType::Directory } else { FileType::RegularFile },
        perm: if root { 0o555 } else { 0o444 },
        nlink: if root { 2 } else { 1 }, uid: 0, gid: 0, rdev: 0, blksize: 4096, flags: 0,
    }
}

// [impl -> dsn~xlb-fuse-snapshots~1]
impl Filesystem for ModelMount {
    fn lookup(&self, _request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(hash) = name.to_str().and_then(|name| BlakeHash::from_hex(name).ok()) else {
            return reply.error(Errno::ENOENT);
        };
        if parent != INodeNo::ROOT {
            return reply.error(Errno::ENOTDIR);
        }
        let mount = self.clone();
        self.runtime.spawn(async move {
            match mount.resolve(hash).await {
                Ok(Some((ino, file))) => reply.entry(&ENTRY_TTL, &attr(ino, file.index.size_bytes), Generation(0)),
                Ok(None) => reply.error(Errno::ENOENT),
                Err(error) => {
                    tracing::warn!(%hash, %error, "resolve mounted model");
                    reply.error(Errno::EIO);
                }
            }
        });
    }

    fn getattr(&self, _request: &Request, ino: INodeNo, _file: Option<FileHandle>, reply: ReplyAttr) {
        if ino == INodeNo::ROOT {
            return reply.attr(&ENTRY_TTL, &attr(ino, 0));
        }
        let Some(hash) = self.hash(ino) else { return reply.error(Errno::ENOENT); };
        let mount = self.clone();
        self.runtime.spawn(async move {
            match mount.resolve(hash).await {
                Ok(Some((_, file))) => reply.attr(&ENTRY_TTL, &attr(ino, file.index.size_bytes)),
                Ok(None) => reply.error(Errno::ENOENT),
                Err(error) => {
                    tracing::warn!(%hash, %error, "resolve mounted model attributes");
                    reply.error(Errno::EIO);
                }
            }
        });
    }

    fn readdir(&self, _request: &Request, ino: INodeNo, _file: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        if ino != INodeNo::ROOT {
            return reply.error(Errno::ENOTDIR);
        }
        let entries = self.entries.read().unwrap();
        let names = [".".into(), "..".into()].into_iter()
            .chain(entries.files.iter().map(|(hash, _)| hash.to_hex()));
        for (position, name) in names.enumerate().skip(offset as usize) {
            let (ino, kind) = if position < 2 {
                (INodeNo::ROOT, FileType::Directory)
            } else {
                (INodeNo(position as u64), FileType::RegularFile)
            };
            if reply.add(ino, position as u64 + 1, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn read(&self, _request: &Request, ino: INodeNo, _file: FileHandle, offset: u64, size: u32,
        _flags: OpenFlags, _lock: Option<LockOwner>, reply: ReplyData,
    ) {
        let Some(hash) = self.hash(ino) else {
            return reply.error(Errno::ENOENT);
        };
        let mount = self.clone();
        self.runtime.spawn(async move {
            let result = async {
                let (_, file) = mount.resolve(hash).await?.context("model has no CAS index")?;
                file.read(offset, size).await.map_err(|error| mount.check_read(ino, &file, error))
            }.await;
            match result {
                Ok(bytes) => reply.data(&bytes),
                Err(error) => {
                    tracing::warn!(%ino, %error, "read mounted model");
                    reply.error(Errno::EIO);
                }
            }
        });
    }
}
