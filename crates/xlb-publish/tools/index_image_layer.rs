use std::{
    collections::BTreeMap,
    io::{self, Read},
};

use anyhow::{Context, Result, bail, ensure};
use xlb_core::{
    BlakeHash,
    chunks::{CAS_CHUNK_BYTES, ChunkIndex},
    layer::{LayerEntry, LayerEntryKind, LayerIndex},
};

struct ArchiveHashes<R> {
    reader: R,
    diff_id: blake3::Hasher,
    chunk: blake3::Hasher,
    chunks: Vec<BlakeHash>,
    size: u64,
}

impl<R: Read> Read for ArchiveHashes<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = self.reader.read(output)?;
        let mut bytes = &output[..count];
        self.diff_id.update(bytes);
        while !bytes.is_empty() {
            let remaining = CAS_CHUNK_BYTES - self.size % CAS_CHUNK_BYTES;
            let length = bytes.len().min(remaining as usize);
            self.chunk.update(&bytes[..length]);
            self.size += length as u64;
            bytes = &bytes[length..];
            if self.size.is_multiple_of(CAS_CHUNK_BYTES) {
                self.chunks.push(BlakeHash::from_bytes(*self.chunk.finalize().as_bytes()));
                self.chunk.reset();
            }
        }
        Ok(count)
    }
}

/// Derive the filesystem index and diff ID from exactly the same archive bytes.
// [impl -> dsn~xlb-fuse-snapshots~2]
pub fn read_layer(reader: impl Read) -> Result<(LayerIndex, blake3::Hash)> {
    let mut archive = tar::Archive::new(ArchiveHashes {
        reader, diff_id: blake3::Hasher::new(), chunk: blake3::Hasher::new(), chunks: Vec::new(), size: 0,
    });
    let mut entries: BTreeMap<Vec<u8>, LayerEntry> = BTreeMap::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let mut path = relative_path(&entry.path_bytes())?;
        let mut xattrs = BTreeMap::new();
        if let Some(extensions) = entry.pax_extensions()? {
            for extension in extensions {
                let extension = extension?;
                if let Some(name) = extension.key_bytes().strip_prefix(b"SCHILY.xattr.") {
                    xattrs.insert(std::str::from_utf8(name)?.to_owned(), extension.value_bytes().to_vec());
                }
                ensure!(!extension.key_bytes().starts_with(b"GNU.sparse."), "OCI sparse tar entries require an extent map");
            }
        }
        let header = entry.header();
        let mut name = path.rsplit(|&byte| byte == b'/');
        let basename = name.next().unwrap_or_default();
        let kind = if basename == b".wh..wh..opq" {
            let parent_length = path.len().saturating_sub(basename.len() + 1);
            path.truncate(parent_length);
            if let Some(existing) = entries.get_mut(&path) {
                existing.kind = LayerEntryKind::Directory { opaque: true };
                continue;
            }
            entries.insert(path.clone(), LayerEntry {
                path, mode: 0o755, uid: 0, gid: 0, modified: 0, xattrs: BTreeMap::new(),
                kind: LayerEntryKind::Directory { opaque: true },
            });
            continue;
        } else if let Some(target) = basename.strip_prefix(b".wh.") {
            ensure!(!target.is_empty() && target != b"." && target != b"..", "invalid OCI whiteout path");
            let mut target_path = path[..path.len() - basename.len()].to_vec();
            target_path.extend_from_slice(target);
            path = target_path;
            if entries.contains_key(&path) { continue; }
            LayerEntryKind::Whiteout
        } else {
            match header.entry_type() {
                tar::EntryType::Regular => LayerEntryKind::File { offset: entry.raw_file_position(), size: entry.size() },
                tar::EntryType::Directory => LayerEntryKind::Directory { opaque: entries.get(&path)
                    .is_some_and(|entry| matches!(entry.kind, LayerEntryKind::Directory { opaque: true })) },
                tar::EntryType::Symlink => LayerEntryKind::Symlink(entry.link_name_bytes().context("symlink has no target")?.into_owned()),
                tar::EntryType::Link => LayerEntryKind::Hardlink(relative_path(&entry.link_name_bytes().context("hardlink has no target")?)?),
                tar::EntryType::Fifo => LayerEntryKind::Fifo,
                kind @ (tar::EntryType::Char | tar::EntryType::Block) => LayerEntryKind::Device {
                    block: kind == tar::EntryType::Block,
                    major: header.device_major()?.context("device has no major number")?,
                    minor: header.device_minor()?.context("device has no minor number")?,
                },
                kind => bail!("unsupported OCI tar entry type {kind:?}"),
            }
        };
        entries.insert(path.clone(), LayerEntry {
            path, mode: (header.mode()? & 0o7777) as u16,
            uid: u32::try_from(header.uid()?)?, gid: u32::try_from(header.gid()?)?,
            modified: header.mtime()?, xattrs, kind,
        });
    }
    let mut reader = archive.into_inner();
    io::copy(&mut reader, &mut io::sink())?;
    if !reader.size.is_multiple_of(CAS_CHUNK_BYTES) {
        reader.chunks.push(BlakeHash::from_bytes(*reader.chunk.finalize().as_bytes()));
    }
    let archive = ChunkIndex { chunks: reader.chunks, size_bytes: reader.size };
    let diff_id = reader.diff_id.finalize();
    let index = LayerIndex {
        archive, diff_id: BlakeHash::from_bytes(*diff_id.as_bytes()), entries: entries.into_values().collect(),
    };
    Ok((index, diff_id))
}

fn relative_path(path: &[u8]) -> Result<Vec<u8>> {
    ensure!(!path.starts_with(b"/") && !path.contains(&0), "invalid absolute or NUL-containing OCI path");
    let components = path.split(|&byte| byte == b'/').filter(|part| !part.is_empty() && *part != b".");
    let mut normalized = Vec::with_capacity(path.len());
    for part in components {
        ensure!(part != b"..", "OCI path traverses outside its layer");
        if !normalized.is_empty() { normalized.push(b'/'); }
        normalized.extend_from_slice(part);
    }
    Ok(normalized)
}
