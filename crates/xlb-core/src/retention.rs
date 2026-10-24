//! Platform-neutral cache retention: pinning, release rotation, and an LRU
//! byte budget that applies only to unpinned entries.
//!
//! This module is pure bookkeeping — no filesystem, no tokio, no clocks — so
//! the on-disk and in-memory caches share it and an OPFS browser cache
//! (R002-F4) can reuse it unchanged on wasm32. A cache owns a
//! [`RetentionIndex`], reports sizes into it, and performs whatever physical
//! deletion the returned eviction lists ask for.
//!
//! ## What is protected
//!
//! A blob is *pinned* if any of these hold:
//!
//! - it was pinned by hash ([`RetentionIndex::pin`]);
//! - it belongs to a named pin set ([`RetentionIndex::pin_set`]);
//! - it is listed by the current release manifest or one of the previous
//!   `keep_releases` manifests ([`RetentionIndex::record_release`]).
//!
//! Pins may be declared before the blob arrives; they take effect on insert.
//! Pinned entries are never evicted and do not count against the budget.
//! `budget_bytes` bounds the *unpinned* footprint only.
//!
//! Pin state here is in-memory bookkeeping. Persistence is the caller's: a
//! cache that reopens declares its pins on a fresh index *before* replaying
//! the stored entries into it (see `RetainingCache::open`), so a pinned blob
//! is never evicted by the rebuild itself. The native `xlb` crate persists
//! named pin sets ("pinned trees", R1003) next to its disk cache that way.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::hash::BlakeHash;

/// Per-`AssetClass` retention settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// LRU byte budget for **unpinned** entries.
    pub budget_bytes: u64,
    /// How many releases *before* the current one stay protected. `1` keeps
    /// current + previous, so an update never strands a rollback.
    pub keep_releases: usize,
}

impl RetentionPolicy {
    pub const fn new(budget_bytes: u64, keep_releases: usize) -> Self {
        Self {
            budget_bytes,
            keep_releases,
        }
    }
}

/// Occupancy split by protection status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RetentionUsage {
    pub entries: u64,
    pub bytes: u64,
    pub pinned_entries: u64,
    pub pinned_bytes: u64,
}

/// Retention bookkeeping for one cache. Not thread-safe; callers wrap it in
/// their own lock.
#[derive(Debug)]
pub struct RetentionIndex {
    policy: RetentionPolicy,
    /// Per-entry size.
    entries: HashMap<BlakeHash, u64>,
    /// Front = least recently used. Same keys as `entries`, no duplicates.
    lru: VecDeque<BlakeHash>,
    total_bytes: u64,
    pinned_bytes: u64,
    pinned_entries: u64,
    pins: HashSet<BlakeHash>,
    sets: HashMap<String, HashSet<BlakeHash>>,
    /// Oldest first; length capped at `keep_releases + 1`.
    releases: VecDeque<(String, HashSet<BlakeHash>)>,
    /// Union of pins, sets and retained releases. Recomputed on change.
    protected: HashSet<BlakeHash>,
}

impl RetentionIndex {
    pub fn new(policy: RetentionPolicy) -> Self {
        Self {
            policy,
            entries: HashMap::new(),
            lru: VecDeque::new(),
            total_bytes: 0,
            pinned_bytes: 0,
            pinned_entries: 0,
            pins: HashSet::new(),
            sets: HashMap::new(),
            releases: VecDeque::new(),
            protected: HashSet::new(),
        }
    }

    pub fn policy(&self) -> RetentionPolicy {
        self.policy
    }

    pub fn contains(&self, hash: &BlakeHash) -> bool {
        self.entries.contains_key(hash)
    }

    pub fn is_pinned(&self, hash: &BlakeHash) -> bool {
        self.protected.contains(hash)
    }

    pub fn usage(&self) -> RetentionUsage {
        RetentionUsage {
            entries: self.entries.len() as u64,
            bytes: self.total_bytes,
            pinned_entries: self.pinned_entries,
            pinned_bytes: self.pinned_bytes,
        }
    }

    /// Would an insert of `size` bytes for `hash` be admitted at all? An
    /// unpinned blob larger than the whole budget is refused (it would evict
    /// everything and still not fit); a pinned one is always admitted.
    pub fn admits(&self, hash: &BlakeHash, size: u64) -> bool {
        self.protected.contains(hash) || size <= self.policy.budget_bytes
    }

    /// Mark `hash` most-recently-used.
    pub fn touch(&mut self, hash: &BlakeHash) {
        if let Some(pos) = self.lru.iter().position(|h| h == hash) {
            self.lru.remove(pos);
            self.lru.push_back(*hash);
        }
    }

    /// Forget `hash` (corrupt or vanished). Returns whether it was present.
    pub fn remove(&mut self, hash: &BlakeHash) -> bool {
        let Some(size) = self.entries.remove(hash) else {
            return false;
        };
        self.total_bytes = self.total_bytes.saturating_sub(size);
        if self.protected.contains(hash) {
            self.pinned_bytes = self.pinned_bytes.saturating_sub(size);
            self.pinned_entries = self.pinned_entries.saturating_sub(1);
        }
        if let Some(pos) = self.lru.iter().position(|h| h == hash) {
            self.lru.remove(pos);
        }
        true
    }

    /// Record `hash` with `size` bytes as most-recently-used (replacing any
    /// existing entry), then evict unpinned LRU entries until the unpinned
    /// footprint fits the budget. Returns the evicted hashes; the caller
    /// deletes their bytes. Never evicts `hash` itself.
    pub fn insert(&mut self, hash: BlakeHash, size: u64) -> Vec<BlakeHash> {
        self.remove(&hash);
        self.entries.insert(hash, size);
        self.lru.push_back(hash);
        self.total_bytes += size;
        if self.protected.contains(&hash) {
            self.pinned_bytes += size;
            self.pinned_entries += 1;
        }
        self.enforce_budget(Some(&hash))
    }

    /// Pin one blob by hash.
    pub fn pin(&mut self, hash: BlakeHash) {
        if self.pins.insert(hash) {
            self.recompute();
        }
    }

    /// Drop a hash pin. Returns blobs evicted because the budget is now
    /// enforceable against them.
    pub fn unpin(&mut self, hash: &BlakeHash) -> Vec<BlakeHash> {
        if self.pins.remove(hash) {
            self.recompute();
        }
        self.enforce_budget(None)
    }

    /// Define (or replace) a named pin set.
    pub fn pin_set(
        &mut self,
        name: impl Into<String>,
        hashes: impl IntoIterator<Item = BlakeHash>,
    ) {
        self.sets.insert(name.into(), hashes.into_iter().collect());
        self.recompute();
    }

    /// Remove a named pin set.
    pub fn unpin_set(&mut self, name: &str) -> Vec<BlakeHash> {
        if self.sets.remove(name).is_some() {
            self.recompute();
        }
        self.enforce_budget(None)
    }

    /// Register `release` as the current release with the blobs its manifest
    /// lists. Releases older than `keep_releases` back are released.
    /// Re-recording an existing release name replaces its hashes in place
    /// and does not rotate. Returns blobs evicted by the rotation.
    pub fn record_release(
        &mut self,
        release: impl Into<String>,
        hashes: impl IntoIterator<Item = BlakeHash>,
    ) -> Vec<BlakeHash> {
        let release = release.into();
        let hashes: HashSet<BlakeHash> = hashes.into_iter().collect();
        if let Some(slot) = self.releases.iter_mut().find(|(n, _)| *n == release) {
            slot.1 = hashes;
        } else {
            self.releases.push_back((release, hashes));
            while self.releases.len() > self.policy.keep_releases + 1 {
                self.releases.pop_front();
            }
        }
        self.recompute();
        self.enforce_budget(None)
    }

    /// The hashes of named pin set `name`, if defined.
    pub fn pin_set_members(&self, name: &str) -> Option<&HashSet<BlakeHash>> {
        self.sets.get(name)
    }

    /// How much of pin set `name` is actually held, if the set is defined.
    pub fn pin_set_usage(&self, name: &str) -> Option<PinSetUsage> {
        let set = self.sets.get(name)?;
        let mut u = PinSetUsage {
            members: set.len() as u64,
            ..Default::default()
        };
        for h in set {
            if let Some(size) = self.entries.get(h) {
                u.present += 1;
                u.present_bytes += size;
            }
        }
        Some(u)
    }

    /// Names of the retained releases, oldest first (current last).
    pub fn releases(&self) -> impl Iterator<Item = &str> {
        self.releases.iter().map(|(n, _)| n.as_str())
    }

    fn recompute(&mut self) {
        let mut p: HashSet<BlakeHash> = self.pins.clone();
        for s in self.sets.values() {
            p.extend(s.iter().copied());
        }
        for (_, r) in &self.releases {
            p.extend(r.iter().copied());
        }
        self.protected = p;
        self.pinned_bytes = 0;
        self.pinned_entries = 0;
        for (h, sz) in &self.entries {
            if self.protected.contains(h) {
                self.pinned_bytes += sz;
                self.pinned_entries += 1;
            }
        }
    }

    fn enforce_budget(&mut self, keep: Option<&BlakeHash>) -> Vec<BlakeHash> {
        let mut evicted = Vec::new();
        let budget = self.policy.budget_bytes;
        while self.total_bytes - self.pinned_bytes > budget {
            let Some(pos) = self
                .lru
                .iter()
                .position(|h| !self.protected.contains(h) && Some(h) != keep)
            else {
                break;
            };
            let victim = self.lru.remove(pos).expect("position is in range");
            if let Some(size) = self.entries.remove(&victim) {
                self.total_bytes = self.total_bytes.saturating_sub(size);
            }
            evicted.push(victim);
        }
        evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u8) -> BlakeHash {
        BlakeHash::hash(&[n])
    }

    fn idx(budget: u64, keep: usize) -> RetentionIndex {
        RetentionIndex::new(RetentionPolicy::new(budget, keep))
    }

    #[test]
    fn plain_lru_without_pins() {
        let mut i = idx(100, 1);
        assert!(i.insert(h(1), 40).is_empty());
        assert!(i.insert(h(2), 40).is_empty());
        assert_eq!(i.insert(h(3), 40), vec![h(1)]);
    }

    #[test]
    fn pinned_entry_survives_and_does_not_count_against_budget() {
        let mut i = idx(100, 1);
        i.pin(h(1));
        i.insert(h(1), 500);
        assert!(
            i.insert(h(2), 60).is_empty(),
            "pinned bytes are outside the budget"
        );
        assert_eq!(i.insert(h(3), 60), vec![h(2)]);
        assert!(i.contains(&h(1)));
        assert_eq!(i.usage().pinned_bytes, 500);
    }

    #[test]
    fn pin_before_arrival_and_pin_after_arrival_both_protect() {
        let mut i = idx(50, 0);
        i.insert(h(1), 40);
        i.pin(h(1));
        i.pin(h(2));
        i.insert(h(2), 40);
        assert!(i.insert(h(3), 40).is_empty());
        assert!(i.contains(&h(1)) && i.contains(&h(2)));
    }

    #[test]
    fn unpin_makes_entry_evictable_and_enforces_budget() {
        let mut i = idx(50, 0);
        i.pin(h(1));
        i.insert(h(1), 40);
        i.insert(h(2), 40);
        assert_eq!(i.unpin(&h(1)), vec![h(1)], "h1 is the older unpinned entry");
    }

    #[test]
    fn pin_set_protects_until_removed() {
        let mut i = idx(50, 0);
        i.pin_set("fonts", [h(1), h(2)]);
        i.insert(h(1), 40);
        i.insert(h(2), 40);
        i.insert(h(3), 40);
        assert!(i.contains(&h(1)) && i.contains(&h(2)));
        assert!(i.unpin_set("fonts").contains(&h(1)));
    }

    #[test]
    fn keeps_current_plus_n_previous_releases() {
        let mut i = idx(10, 1);
        i.insert(h(1), 8);
        i.record_release("r1", [h(1)]);
        i.insert(h(2), 8);
        i.record_release("r2", [h(2)]);
        i.insert(h(3), 8);
        assert!(i.contains(&h(1)) && i.contains(&h(2)) && i.contains(&h(3)));
        let ev = i.record_release("r3", [h(3)]);
        assert!(ev.is_empty(), "unpinned footprint still fits the budget");
        assert!(!i.is_pinned(&h(1)), "r1 is two releases back");
        assert!(i.is_pinned(&h(2)) && i.is_pinned(&h(3)));
        assert_eq!(i.insert(h(4), 8), vec![h(1)], "r1 is now the LRU victim");
        assert_eq!(i.releases().collect::<Vec<_>>(), ["r2", "r3"]);
    }

    #[test]
    fn re_recording_a_release_does_not_rotate() {
        let mut i = idx(100, 1);
        i.record_release("r1", [h(1)]);
        i.record_release("r2", [h(2)]);
        i.record_release("r2", [h(2), h(3)]);
        assert_eq!(i.releases().collect::<Vec<_>>(), ["r1", "r2"]);
        assert!(i.is_pinned(&h(1)) && i.is_pinned(&h(3)));
    }

    #[test]
    fn blob_in_two_protections_survives_losing_one() {
        let mut i = idx(10, 0);
        i.pin(h(1));
        i.record_release("r1", [h(1)]);
        i.insert(h(1), 8);
        i.record_release("r2", [h(2)]);
        assert!(i.contains(&h(1)), "still hash-pinned");
    }

    #[test]
    fn oversized_unpinned_is_refused_pinned_is_admitted() {
        let mut i = idx(10, 0);
        assert!(!i.admits(&h(1), 11));
        i.pin(h(1));
        assert!(i.admits(&h(1), 11));
    }

    #[test]
    fn the_inserted_entry_is_never_its_own_victim() {
        let mut i = idx(10, 0);
        i.pin(h(1));
        i.insert(h(1), 8);
        // Over budget with only the new entry evictable: it stays.
        assert!(i.insert(h(2), 10).is_empty());
        assert!(i.contains(&h(2)));
    }

    #[test]
    fn remove_adjusts_pinned_accounting() {
        let mut i = idx(100, 0);
        i.pin(h(1));
        i.insert(h(1), 30);
        assert!(i.remove(&h(1)));
        assert_eq!(i.usage(), RetentionUsage::default());
    }
}
