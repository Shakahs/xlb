//! The node's complete chunk cache and its discovery checkpoint.

use std::{
    ffi::CString,
    hash::{BuildHasherDefault, Hash, Hasher},
    io,
    mem::MaybeUninit,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

use bao_tree::{BlockSize, io::outboard::PreOrderMemOutboard};
use bytes::Bytes;
use dashmap::DashSet;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FileDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, IoEngineConfig, RecoverMode,
};
use serde::{Deserialize, Serialize};
use xlb_core::{BlakeHash, chunks::CAS_CHUNK_BYTES};

use crate::{Error, Result};

const MIB: usize = 1024 * 1024;
const CACHE_BLOCK_BYTES: usize = 32 * MIB;
const MAX_OUTBOARD_BYTES: usize = 32_704;
const MEMORY_SHARDS: usize = 8;
const DEVICE_PATH: &str = ".foyer/chunks";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum CasKey {
    Chunk([u8; 32]),
    RootManifest,
}

impl Hash for CasKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Chunk(digest) => state.write(digest),
            Self::RootManifest => state.write(b"xlb-cas-root"),
        }
    }
}

#[derive(Debug, Default)]
struct CasHasher(blake3::Hasher);

impl Hasher for CasHasher {
    fn finish(&self) -> u64 {
        let digest = self.0.finalize();
        let mut index_hash = [0; 8];
        index_hash.copy_from_slice(&digest.as_bytes()[..8]);
        u64::from_le_bytes(index_hash)
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifiedChunk {
    body: Bytes,
    outboard: Bytes,
}

impl VerifiedChunk {
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn outboard(&self) -> &Bytes {
        &self.outboard
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CasValue {
    Chunk(VerifiedChunk),
    RootManifest(Bytes),
}

impl CasValue {
    fn weight(&self) -> usize {
        match self {
            Self::Chunk(chunk) => chunk.body.len() + chunk.outboard.len(),
            Self::RootManifest(digests) => digests.len(),
        }
    }
}

/// Foyer owns payload retention. Candidate digests are used for announcements.
pub struct CasCache {
    cache: HybridCache<CasKey, CasValue, BuildHasherDefault<CasHasher>>,
    candidates: DashSet<[u8; 32]>,
    dirty: AtomicBool,
}

// [impl -> dsn~xlb-foyer-cas~1]
impl CasCache {
    /// Size the cache from the scratch filesystem and installed RAM.
    pub fn talos_budgets(cache_dir: &Path) -> Result<(usize, u64)> {
        std::fs::create_dir_all(cache_dir)?;
        let path = CString::new(cache_dir.as_os_str().as_bytes()).map_err(io::Error::other)?;
        let mut volume = MaybeUninit::<libc::statvfs>::uninit();
        // statvfs initializes the output on success. The pathname is NUL terminated.
        let volume = unsafe {
            if libc::statvfs(path.as_ptr(), volume.as_mut_ptr()) != 0 {
                return Err(io::Error::last_os_error().into());
            }
            volume.assume_init()
        };
        let already_allocated = match std::fs::metadata(cache_dir.join(DEVICE_PATH)) {
            Ok(file) => file.blocks() * 512,
            Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let capacity = volume.f_blocks * volume.f_frsize;
        let available = volume.f_bavail * volume.f_frsize + already_allocated;
        let disk = (capacity - capacity.div_ceil(4)).min(available);
        // sysconf returns -1 when a value is unavailable.
        let (pages, page_bytes) = unsafe {
            (libc::sysconf(libc::_SC_PHYS_PAGES), libc::sysconf(libc::_SC_PAGESIZE))
        };
        if pages <= 0 || page_bytes <= 0 {
            return Err(Error::Config("cannot determine total RAM for the Talos CAS".into()));
        }
        let memory = (pages as usize).checked_mul(page_bytes as usize)
            .ok_or_else(|| Error::Config("physical RAM exceeds the platform limit".into()))? / 2;
        Ok((memory, disk))
    }

    pub async fn open(root: &Path, memory: usize, disk: usize) -> Result<Self> {
        #[cfg(not(test))]
        let io = Box::new(foyer::UringIoEngineConfig::new());
        #[cfg(test)]
        let io = Box::new(foyer::PsyncIoEngineConfig::new());
        Self::open_with_io(root, memory, disk, io).await
    }

    async fn open_with_io(
        root: &Path,
        memory: usize,
        disk: usize,
        io: Box<dyn IoEngineConfig>,
    ) -> Result<Self> {
        let queue_budget = memory / 8;
        let resident = memory.saturating_sub(2 * CACHE_BLOCK_BYTES + queue_budget);
        if resident / MEMORY_SHARDS < CAS_CHUNK_BYTES as usize + MAX_OUTBOARD_BYTES {
            return Err(Error::Config("Foyer memory budget must fit one complete CAS chunk per shard".into()));
        }
        if disk / CACHE_BLOCK_BYTES < 3 {
            return Err(Error::Config("Foyer disk budget must fit its write and reclaim blocks".into()));
        }
        let path = root.join(DEVICE_PATH);
        std::fs::create_dir_all(path.parent().expect("Foyer device directory"))?;
        let device = FileDeviceBuilder::new(&path).with_capacity(disk).build()
            .map_err(|error| Error::Store(format!("open Foyer device at {}: {error}", root.display())))?;
        let cache = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnEviction)
            .with_flush_on_close(true)
            .memory(resident)
            .with_shards(MEMORY_SHARDS)
            .with_hash_builder(BuildHasherDefault::<CasHasher>::default())
            .with_weighter(|_, value: &CasValue| value.weight())
            .storage()
            .with_io_engine_config(io)
            .with_engine_config(
                BlockEngineConfig::new(device)
                    .with_block_size(CACHE_BLOCK_BYTES)
                    .with_buffer_pool_size(CACHE_BLOCK_BYTES)
                    .with_submit_queue_size_threshold(queue_budget),
            )
            .with_recover_mode(RecoverMode::Quiet)
            .build().await
            .map_err(|error| Error::Store(format!("recover Foyer cache at {}: {error}", root.display())))?;
        let cache = Self {
            cache,
            candidates: DashSet::new(),
            dirty: AtomicBool::new(false),
        };
        cache.restore().await;
        Ok(cache)
    }

    async fn restore(&self) {
        let result = self.cache.storage().load(&CasKey::RootManifest).await;
        let manifest = match result {
            Ok(result) => result.kv(),
            Err(error) => {
                tracing::warn!(%error, "read XLB CAS root manifest");
                return;
            }
        };
        let Some((CasKey::RootManifest, CasValue::RootManifest(body))) = manifest else {
            return;
        };
        let (digests, remainder) = body.as_chunks::<32>();
        if !remainder.is_empty() {
            tracing::warn!("invalid XLB CAS root manifest length");
            return;
        }
        for &digest in digests {
            if self.cache.contains(&CasKey::Chunk(digest)) {
                self.candidates.insert(digest);
            } else {
                self.dirty.store(true, Ordering::Relaxed);
            }
        }
    }

    fn remember(&self, digest: [u8; 32]) {
        if self.candidates.insert(digest) {
            self.dirty.store(true, Ordering::Release);
        }
    }

    pub async fn get(&self, hash: BlakeHash) -> Result<Option<VerifiedChunk>> {
        let digest = *hash.as_bytes();
        let entry = self.cache.get(&CasKey::Chunk(digest)).await
            .map_err(|error| Error::Store(format!("read Foyer chunk {hash}: {error}")))?;
        let Some(entry) = entry else {
            if self.candidates.remove(&digest).is_some() {
                self.dirty.store(true, Ordering::Release);
            }
            return Ok(None);
        };
        let CasValue::Chunk(chunk) = entry.value() else {
            return Err(Error::Store(format!("unexpected Foyer value for chunk {hash}")));
        };
        self.remember(digest);
        Ok(Some(chunk.clone()))
    }

    pub async fn insert(&self, hash: BlakeHash, length: u64, body: Bytes) -> Result<VerifiedChunk> {
        if length == 0 || length > CAS_CHUNK_BYTES || length != body.len() as u64 {
            return Err(Error::Store(format!("CAS chunk {hash} has an unexpected length")));
        }
        let (chunk, actual) = tokio::task::spawn_blocking(move || {
            let proof = PreOrderMemOutboard::create(&body, BlockSize::from_chunk_log(4));
            let actual = BlakeHash::from_bytes(*proof.root.as_bytes());
            (VerifiedChunk { body, outboard: proof.data.into() }, actual)
        }).await.map_err(|error| Error::Store(format!("build CAS chunk outboard: {error}")))?;
        if actual != hash {
            return Err(Error::HashMismatch { expected: hash, actual });
        }
        let digest = *hash.as_bytes();
        self.cache.insert(CasKey::Chunk(digest), CasValue::Chunk(chunk.clone()));
        self.remember(digest);
        Ok(chunk)
    }

    /// Return candidates without loading the chunk payloads from disk.
    pub fn complete_hashes(&self) -> Vec<BlakeHash> {
        self.prune();
        self.candidates.iter().map(|digest| BlakeHash::from_bytes(*digest)).collect()
    }

    fn prune(&self) {
        self.candidates.retain(|digest| {
            let present = self.cache.contains(&CasKey::Chunk(*digest));
            if !present {
                self.dirty.store(true, Ordering::Release);
            }
            present
        });
    }

    pub(crate) fn occupancy(&self) -> (u64, u64) {
        (self.cache.memory().entries() as u64, self.cache.memory().usage() as u64)
    }

    pub(crate) fn may_hold(&self, hash: BlakeHash) -> bool {
        self.cache.contains(&CasKey::Chunk(*hash.as_bytes()))
    }

    /// Checkpoint only on the discovery task or after it has stopped.
    pub async fn checkpoint(&self) -> Result<()> {
        self.prune();
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        let mut bytes = Vec::with_capacity(self.candidates.len() * 32);
        for digest in self.candidates.iter() {
            bytes.extend_from_slice(&*digest);
        }
        let bytes = Bytes::from(bytes);
        self.cache.storage_writer(CasKey::RootManifest).force()
            .insert(CasValue::RootManifest(bytes.clone()));
        self.cache.storage().wait().await;
        let saved = self.cache.storage().load(&CasKey::RootManifest).await
            .ok().and_then(|load| load.kv());
        match saved {
            Some((_, CasValue::RootManifest(saved))) if saved == bytes => Ok(()),
            _ => {
                self.dirty.store(true, Ordering::Release);
                Err(Error::Store("Foyer did not persist the CAS root manifest".into()))
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        self.cache.flush_if(|key, _| matches!(key, CasKey::Chunk(_))).await;
        self.cache.storage().wait().await;
        self.dirty.store(true, Ordering::Release);
        let checkpoint = self.checkpoint().await;
        self.cache.close().await
            .map_err(|error| Error::Store(format!("close Foyer cache: {error}")))?;
        checkpoint
    }
}

#[cfg(test)]
mod tests {
    use foyer::PsyncIoEngineConfig;

    use super::*;

    // [utest -> dsn~xlb-foyer-cas~1]
    #[tokio::test]
    async fn root_restores_only_complete_verified_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let open = || CasCache::open_with_io(
            dir.path(), 160 * MIB, 128 * MIB, Box::new(PsyncIoEngineConfig::new()),
        );
        let cache = open().await.unwrap();
        let full = Bytes::from(vec![13; CAS_CHUNK_BYTES as usize]);
        let hash = BlakeHash::hash(&full);
        assert!(cache.insert(hash, CAS_CHUNK_BYTES, full.slice(..100)).await.is_err());
        assert!(cache.get(hash).await.unwrap().is_none());
        let held = cache.insert(hash, CAS_CHUNK_BYTES, full.clone()).await.unwrap();
        let tail = Bytes::from_static(b"complete final chunk");
        let tail_hash = BlakeHash::hash(&tail);
        cache.insert(tail_hash, tail.len() as u64, tail.clone()).await.unwrap();
        cache.checkpoint().await.unwrap();
        let hot = cache.cache.storage().load(&CasKey::Chunk(*hash.as_bytes())).await.unwrap();
        assert!(hot.is_miss(), "checkpointing does not spill a hot chunk");

        let shard = |digest: BlakeHash| {
            cache.cache.memory().hash(&CasKey::Chunk(*digest.as_bytes())) as usize % MEMORY_SHARDS
        };
        let mut pressure = full.to_vec();
        let replacement = (0..=u8::MAX).find_map(|byte| {
            pressure[0] = byte;
            let digest = BlakeHash::hash(&pressure);
            (digest != hash && shard(digest) == shard(hash)).then_some(digest)
        }).expect("a distinct chunk in the same cache shard");
        cache.insert(replacement, CAS_CHUNK_BYTES, Bytes::from(pressure)).await.unwrap();
        assert!(!cache.cache.memory().contains(&CasKey::Chunk(*hash.as_bytes())));
        cache.cache.storage().wait().await;
        assert!(cache.cache.storage().load(&CasKey::Chunk(*hash.as_bytes())).await.unwrap().kv().is_some());
        assert_eq!(held.body(), &full);

        cache.close().await.unwrap();
        drop(cache);
        assert_eq!(held.body(), &full);

        let reopened = open().await.unwrap();
        let digests = reopened.complete_hashes();
        assert_eq!(digests.len(), 3);
        assert!(digests.contains(&hash));
        assert!(digests.contains(&tail_hash));
        assert_eq!(reopened.get(hash).await.unwrap().unwrap().body(), &full);
        assert_eq!(reopened.get(tail_hash).await.unwrap().unwrap().body(), &tail);
        reopened.close().await.unwrap();
    }
}
