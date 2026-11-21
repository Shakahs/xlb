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
use xlb_core::chunks::{CAS_INDEX_LIMIT, ChunkIndex, MODEL_INDEX_PREFIX};

use crate::{chunk_file::ChunkFile, DiskChunks, snapshot_store::write_index, snapshotter::clear_disconnected_mount};

const ENTRY_TTL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct ModelEntries {
    inodes: HashMap<BlakeHash, INodeNo>,
    files: Vec<(BlakeHash, ChunkFile)>,
}

#[derive(Clone)]
pub struct ModelMount {
    root: PathBuf,
    indexes: PathBuf,
    class: AssetClass,
    disk: DiskChunks,
    entries: Arc<RwLock<ModelEntries>>,
    runtime: Handle,
    concurrency: usize,
}

impl ModelMount {
    pub async fn start(root: &Path, class: AssetClass, disk: DiskChunks, concurrency: usize) -> Result<(Self, BackgroundSession)> {
        let indexes = class.cache_dir().join(".model-indexes");
        tokio::fs::create_dir_all(&indexes).await?;
        match tokio::fs::create_dir_all(root).await {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(libc::ENOTCONN) => {}
            Err(error) => return Err(error.into()),
        }
        let mount = Self {
            root: root.to_owned(), indexes, class, disk, entries: Arc::default(), runtime: Handle::current(), concurrency,
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
        let (_, file) = self.resolve(hash).await?.context("model has no CAS index")?;
        let total = file.index.size_bytes;
        let index = &file.index;
        let mut chunks = stream::iter(0..file.index.chunks.len() as u64)
            .map(|ordinal| async move {
                let (chunk, length) = index.chunk(ordinal).context("model CAS index is truncated")?;
                let (result, report) = self.class.indexed_chunk(chunk, length).fetch_to_file_reported(None).await;
                result?;
                ensure!(report.bytes_served == length, "model CAS chunk {chunk} has an unexpected length");
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

    fn file(&self, ino: INodeNo) -> Option<ChunkFile> {
        let index = usize::try_from(ino.0.checked_sub(2)?).ok()?;
        self.entries.read().unwrap().files.get(index).map(|(_, file)| file.clone())
    }

    async fn resolve(&self, hash: BlakeHash) -> Result<Option<(INodeNo, ChunkFile)>> {
        {
            let entries = self.entries.read().unwrap();
            if let Some(&ino) = entries.inodes.get(&hash) {
                return Ok(Some((ino, entries.files[(ino.0 - 2) as usize].1.clone())));
            }
        }
        let path = self.indexes.join(hash.to_hex());
        let (bytes, download) = match tokio::fs::read(&path).await {
            Ok(bytes) => (bytes, false),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let key = format!("{MODEL_INDEX_PREFIX}{hash}");
                let Some(bytes) = self.class.read_origin_object(&key, CAS_INDEX_LIMIT).await? else {
                    return Ok(None);
                };
                (bytes, true)
            }
            Err(error) => return Err(error.into()),
        };
        let index: ChunkIndex = serde_json::from_slice(&bytes).context("parse model CAS index")?;
        let file = ChunkFile::new(Arc::new(index), self.class.clone(), self.disk.clone())?;
        if download {
            tokio::task::spawn_blocking(move || write_index(&path, &bytes)).await??;
        }
        let mut entries = self.entries.write().unwrap();
        if let Some(&ino) = entries.inodes.get(&hash) {
            return Ok(Some((ino, entries.files[(ino.0 - 2) as usize].1.clone())));
        }
        let ino = INodeNo(entries.files.len() as u64 + 2);
        entries.inodes.insert(hash, ino);
        entries.files.push((hash, file.clone()));
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
            reply.attr(&ENTRY_TTL, &attr(ino, 0));
        } else if let Some(file) = self.file(ino) {
            reply.attr(&ENTRY_TTL, &attr(ino, file.index.size_bytes));
        } else {
            reply.error(Errno::ENOENT);
        }
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
        let Some(file) = self.file(ino) else {
            return reply.error(Errno::ENOENT);
        };
        self.runtime.spawn(async move {
            match file.read(offset, size).await {
                Ok(bytes) => reply.data(&bytes),
                Err(error) => {
                    tracing::warn!(%ino, %error, "read mounted model");
                    reply.error(Errno::EIO);
                }
            }
        });
    }
}
