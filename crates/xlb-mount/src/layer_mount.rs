use std::{
    collections::BTreeMap,
    ffi::OsStr,
    os::unix::ffi::OsStrExt,
    path::Path,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use fuser::{
    BackgroundSession, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, Generation,
    INodeNo, LockOwner, MountOption, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyXattr, Request, SessionACL,
};
use tokio::runtime::Handle;
use xlb::AssetClass;
use xlb_core::layer::{LayerEntry, LayerEntryKind, LayerIndex};

use crate::{chunk_file::ChunkFile, DiskChunks};

const ENTRY_TTL: Duration = Duration::from_secs(3600);

struct LayerNode {
    attr: FileAttr,
    kind: LayerEntryKind,
    parent: INodeNo,
    children: BTreeMap<Vec<u8>, INodeNo>,
    xattrs: BTreeMap<String, Vec<u8>>,
}

struct LayerMount {
    archive: ChunkFile,
    nodes: Vec<LayerNode>,
    runtime: Handle,
}

// [impl -> dsn~xlb-fuse-snapshots~1]
pub(crate) fn mount_layer(
    index: LayerIndex, class: AssetClass, disk: DiskChunks, mountpoint: &Path, runtime: Handle,
) -> Result<BackgroundSession> {
    let archive = ChunkFile::new(Arc::new(index.archive), class, disk)?;
    let nodes = layer_nodes(index.entries, archive.index.size_bytes)?;
    let layer = LayerMount { archive, nodes, runtime };
    let mut config = Config::default();
    config.acl = SessionACL::All;
    config.mount_options = vec![MountOption::RO, MountOption::DefaultPermissions,
        MountOption::Exec, MountOption::NoAtime, MountOption::FSName("xlb-layer".into())];
    fuser::spawn_mount(layer, mountpoint, &config).context("mount XLB OCI layer")
}

fn parent_and_name(path: &[u8]) -> (&[u8], &[u8]) {
    match path.iter().rposition(|&byte| byte == b'/') {
        Some(split) => (&path[..split], &path[split + 1..]),
        None => (&[], path),
    }
}

fn layer_nodes(entries: Vec<LayerEntry>, archive_size: u64) -> Result<Vec<LayerNode>> {
    let mut entries: BTreeMap<_, _> = entries.into_iter().map(|entry| (entry.path.clone(), entry)).collect();
    let mut directories = vec![Vec::new()];
    for path in entries.keys() {
        ensure!(!path.contains(&0) && (path.is_empty() || path.split(|&byte| byte == b'/')
            .all(|part| !part.is_empty() && part != b"." && part != b"..")), "invalid path in OCI layer index");
        let mut path = path.as_slice();
        while !path.is_empty() {
            path = parent_and_name(path).0;
            if entries.contains_key(path) { break; }
            directories.push(path.to_vec());
        }
    }
    for path in directories {
        entries.entry(path.clone()).or_insert_with(|| LayerEntry {
            path, mode: 0o755, uid: 0, gid: 0, modified: 0, xattrs: BTreeMap::new(),
            kind: LayerEntryKind::Directory { opaque: false },
        });
    }

    let mut nodes = Vec::new();
    let mut inodes = BTreeMap::new();
    let mut hardlinks = BTreeMap::new();
    for (path, entry) in entries {
        if let LayerEntryKind::Hardlink(target) = entry.kind {
            hardlinks.insert(path, target);
            continue;
        }
        let ino = INodeNo(nodes.len() as u64 + 1);
        nodes.push(LayerNode::new(ino, entry, archive_size)?);
        inodes.insert(path, ino);
    }
    ensure!(inodes.get([].as_slice()) == Some(&INodeNo::ROOT) && nodes[0].attr.kind == FileType::Directory,
        "OCI layer root is not a directory");
    for (path, target) in &hardlinks {
        let mut target = target;
        let mut hops = 0;
        while let Some(next) = hardlinks.get(target) {
            hops += 1;
            ensure!(hops <= hardlinks.len(), "cyclic hardlink in OCI layer");
            target = next;
        }
        let ino = *inodes.get(target).context("OCI layer hardlink target is missing")?;
        let node = &mut nodes[(ino.0 - 1) as usize];
        ensure!(node.attr.kind != FileType::Directory, "OCI layer hardlink targets a directory");
        node.attr.nlink += 1;
        inodes.insert(path.clone(), ino);
    }
    for (path, &ino) in &inodes {
        if path.is_empty() { continue; }
        let (parent, name) = parent_and_name(path);
        let parent = *inodes.get(parent).context("OCI layer parent is missing")?;
        let is_directory = nodes[(ino.0 - 1) as usize].attr.kind == FileType::Directory;
        if is_directory { nodes[(ino.0 - 1) as usize].parent = parent; }
        let parent_node = &mut nodes[(parent.0 - 1) as usize];
        ensure!(parent_node.attr.kind == FileType::Directory, "OCI layer parent is not a directory");
        parent_node.attr.nlink += u32::from(is_directory);
        parent_node.children.insert(name.to_vec(), ino);
    }
    Ok(nodes)
}

impl LayerNode {
    fn new(ino: INodeNo, mut entry: LayerEntry, archive_size: u64) -> Result<Self> {
        let (kind, size, rdev) = match &entry.kind {
            LayerEntryKind::File { offset, size } => {
                ensure!(*offset <= archive_size && *size <= archive_size - offset, "OCI file extent exceeds its archive");
                (FileType::RegularFile, *size, 0)
            }
            LayerEntryKind::Directory { opaque } => {
                if *opaque { entry.xattrs.insert("trusted.overlay.opaque".into(), b"y".to_vec()); }
                (FileType::Directory, 0, 0)
            }
            LayerEntryKind::Symlink(target) => (FileType::Symlink, target.len() as u64, 0),
            LayerEntryKind::Fifo => (FileType::NamedPipe, 0, 0),
            LayerEntryKind::Device { block, major, minor } => (
                if *block { FileType::BlockDevice } else { FileType::CharDevice },
                0,
                u32::try_from(libc::makedev(*major, *minor))?,
            ),
            LayerEntryKind::Whiteout => (FileType::CharDevice, 0, 0),
            LayerEntryKind::Hardlink(_) => unreachable!("hardlinks share inodes"),
        };
        let modified = UNIX_EPOCH.checked_add(Duration::from_secs(entry.modified)).context("invalid OCI modification time")?;
        let attr = FileAttr {
            ino, size, blocks: size.div_ceil(512), kind, rdev,
            atime: modified, mtime: modified, ctime: modified, crtime: modified,
            perm: entry.mode, nlink: if kind == FileType::Directory { 2 } else { 1 },
            uid: entry.uid, gid: entry.gid, blksize: 4096, flags: 0,
        };
        Ok(Self { attr, kind: entry.kind, parent: INodeNo::ROOT, children: BTreeMap::new(), xattrs: entry.xattrs })
    }
}

impl LayerMount {
    fn node(&self, ino: INodeNo) -> Option<&LayerNode> {
        self.nodes.get(usize::try_from(ino.0.checked_sub(1)?).ok()?)
    }
}

impl Filesystem for LayerMount {
    fn lookup(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(node) = self.node(parent) else { return reply.error(Errno::ENOENT); };
        let child = match name.as_bytes() {
            b"." => Some(parent),
            b".." => Some(node.parent),
            name => node.children.get(name).copied(),
        };
        match child.and_then(|ino| self.node(ino)) {
            Some(node) => reply.entry(&ENTRY_TTL, &node.attr, Generation(0)),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn getattr(&self, _: &Request, ino: INodeNo, _: Option<FileHandle>, reply: ReplyAttr) {
        match self.node(ino) {
            Some(node) => reply.attr(&ENTRY_TTL, &node.attr),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn readlink(&self, _: &Request, ino: INodeNo, reply: ReplyData) {
        match self.node(ino).map(|node| &node.kind) {
            Some(LayerEntryKind::Symlink(target)) => reply.data(target),
            _ => reply.error(Errno::EINVAL),
        }
    }

    fn readdir(&self, _: &Request, ino: INodeNo, _: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let Some(node) = self.node(ino) else { return reply.error(Errno::ENOENT); };
        if node.attr.kind != FileType::Directory { return reply.error(Errno::ENOTDIR); }
        let entries = [(b".".as_slice(), ino), (b"..".as_slice(), node.parent)].into_iter()
            .chain(node.children.iter().map(|(name, &child)| (name.as_slice(), child)));
        for (position, (name, child)) in entries.enumerate().skip(offset as usize) {
            let kind = self.nodes[(child.0 - 1) as usize].attr.kind;
            if reply.add(child, position as u64 + 1, kind, OsStr::from_bytes(name)) { break; }
        }
        reply.ok();
    }

    fn read(&self, _: &Request, ino: INodeNo, _: FileHandle, offset: u64, length: u32,
        _: OpenFlags, _: Option<LockOwner>, reply: ReplyData,
    ) {
        let Some(LayerEntryKind::File { offset: start, size }) = self.node(ino).map(|node| &node.kind) else {
            return reply.error(Errno::EINVAL);
        };
        let length = size.saturating_sub(offset).min(u64::from(length)) as u32;
        if length == 0 { return reply.data(&[]); }
        let position = start + offset;
        let archive = self.archive.clone();
        self.runtime.spawn(async move {
            match archive.read(position, length).await {
                Ok(bytes) => reply.data(&bytes),
                Err(error) => {
                    tracing::warn!(%ino, %error, "read OCI layer file");
                    reply.error(Errno::EIO);
                }
            }
        });
    }

    fn getxattr(&self, _: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let Some(node) = self.node(ino) else { return reply.error(Errno::ENOENT); };
        match name.to_str().and_then(|name| node.xattrs.get(name)) {
            Some(value) => reply_xattr(value, size, reply),
            None => reply.error(Errno::ENODATA),
        }
    }

    fn listxattr(&self, _: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let Some(node) = self.node(ino) else { return reply.error(Errno::ENOENT); };
        let names: Vec<_> = node.xattrs.keys().flat_map(|name| name.bytes().chain([0])).collect();
        reply_xattr(&names, size, reply);
    }
}

fn reply_xattr(value: &[u8], size: u32, reply: ReplyXattr) {
    if size == 0 { reply.size(value.len() as u32); }
    else if size as usize >= value.len() { reply.data(value); }
    else { reply.error(Errno::ERANGE); }
}
