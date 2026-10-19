//! # xlb — chunked, Bao-verified blob distribution
//!
//! Reads as **peer-eXchange + LAN + Bao-tree**. Mnemonic: each blob is a
//! xiaolongbao — content sealed in a verified wrapper, served from a kitchen,
//! shareable across tables.
//!
//! Multi-source concurrent fetch (local cache → LAN peer → swarm peer →
//! permanent seed → CDN edge), with per-chunk BLAKE3 Bao-tree verification so
//! chunks pulled from different sources are independently verifiable.
//!
//! **Status: xlb-4.** Core types + iroh-blobs transport (xlb-2) + HTTP CDN
//! fallback + Bao verifier glue (xlb-3) + BandwidthGovernor with
//! battery/metered auto-detection (xlb-4). Full five-tier fetch chain wired
//! per `.yah/docs/architecture/xlb.md`.
//!
//! @yah:ticket(R002-F2, "Ref resolution: Hash, Version(name, semver req), Latest(name, channel) via per-class manifest")
//! @yah:status(review)
//! @yah:at(2026-10-18T18:14:46Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R002)
//! @yah:next("Tier: Warrior")
//! @yah:next("Ref::{Hash, Version(name, semver req), Latest(name, channel)} resolved through a per-class manifest.")
//! @yah:next("Hash refs are immutable; Version/Latest re-resolve on a TTL.")
//! @yah:depends_on(R002-F1)
//! @yah:handoff("Added crates/xlb/src/refs.rs (platform-neutral: no fs/tokio/clock): Ref::{Hash, Version(String, VersionReq), Latest(String, String)}, Manifest JSON (schema=1; release{id,hashes}; pinned_sets; entries{name,version,channels,hash,size}), ParsedManifest (validates versions/hashes up front; resolve picks highest semver; Latest filters by channel), Resolution{hash,fetched_at_ms}, is_expired(fetched_at, now, ttl) with now as a parameter.")
//! @yah:handoff("Added crates/xlb/src/resolver.rs (native): ManifestSource async trait, HttpManifestSource (reqwest, no redirects), ResolverState (manifest + fetched_at, Ref memo, applied pin-set names). lib.rs: AssetClassConfig.manifest_url + resolve_ttl (default 300s); AssetClass::{resolve, resolve_asset, refresh_manifest, set_manifest_source}. Hash refs return immediately and never fetch. Version/Latest memoised; past TTL the manifest is refetched and the memo cleared. If the refetch fails and a manifest is cached, the stale resolution is served (stays expired, retries next call); cold failure errors.")
//! @yah:handoff("Pin re-derivation: each manifest refresh feeds release -> record_release, pinned_sets -> pin_set, and sets the previous manifest had but the new one dropped -> unpin_set. This resolves R002-F1's 'pin state is in-memory only' gotcha; struck it from R002-F1 via board.update.")
//! @yah:handoff("Deps added to crates/xlb/Cargo.toml: semver 1, serde 1 (derive), serde_json 1 (all already in Cargo.lock; lock unchanged by me). Updated examples/register_class.rs for the new config fields. Did not edit noisetable's required_manifest.rs (used only as format reference: release + per-asset blake3/size).")
//! @yah:handoff("Git policy defer, no commit. Suggested: git add crates/xlb/src/refs.rs crates/xlb/src/resolver.rs crates/xlb/src/lib.rs crates/xlb/Cargo.toml crates/xlb/examples/register_class.rs && git commit -m 'xlb: Ref resolution via per-class manifest (R002-F2)'")
//! @yah:verify("cargo test --workspace in oss/xlb: EXIT=0; xlb lib 72 -> 82 (5 refs tests + 5 resolver tests: Hash no-fetch, Version/Latest TTL re-resolve, stale-on-failure, no source, pin re-derivation + dropped-set unpin); full workspace 126 passed vs baseline 116 (log /tmp/r002f2-5.log).")
//! @yah:gotcha("The manifest is a mutable, non-content-addressed pointer; its integrity is only the transport's (TLS). Blobs it names are still BLAKE3-verified at fetch. No signature over the manifest yet.")
//! @yah:gotcha("Wasm-neutrality of refs.rs not compiled against wasm32 (deps: semver, serde, serde_json, thiserror, crate::BlakeHash which lives in the native lib.rs) — R002-F4 extraction must move BlakeHash too.")
//!
//! @yah:ticket(R002-S3, "Spike: does xlb (iroh/iroh-blobs deps) build for wasm32?")
//! @yah:status(review)
//! @yah:at(2026-10-18T18:06:17Z)
//! @yah:kind(spike)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R002)
//! @yah:next("Tier: Warrior")
//! @yah:next("Decides whether the browser runs xlb itself or a thin client sharing xlb's policy types (retention and ref types).")
//! @yah:handoff("VERDICT: browser can run iroh/iroh-blobs themselves, but NOT xlb as-is; recommend a split crate (xlb-core: policy/Ref/verify types, no iroh) shared by a thin wasm client, with OPFS cache (R002-F4) implemented behind a cache trait. Do not try to compile the current xlb crate for wasm32.")
//! @yah:handoff("EVIDENCE: `cargo check -p xlb --target wasm32-unknown-unknown` fails first in mio 1.2.0 (36 errors, mio has no wasm32-unknown-unknown backend). mio reaches xlb via (a) mshr 0.8.47 -> mdns-sd 0.21.4 (mshr/Cargo.toml non-apple dep, unconditional) and (b) tokio 1.52.3 with net-capable features pulled by iroh deps/hyper (iroh-metrics 1.0.2 -> hyper-util). xlb itself also requests tokio `rt-multi-thread`, which tokio rejects on wasm.")
//! @yah:handoff("POSITIVE PROBE: scratch crate with iroh 1.3.0 + iroh-blobs 0.103.1 (default-features=false, no `tls-ring`) + blake3 + bytes passes `cargo check --target wasm32-unknown-unknown` (EXIT=0). Registry source confirms deliberate browser support: iroh-1.3.0/Cargo.toml has target cfg(all(target_family=wasm,target_os=unknown)) deps getrandom(wasm_js), time(wasm-bindgen), wasm-bindgen-futures, and gates noq/portmapper/tokio to non-wasm (~256 wasm cfg hits in iroh/src); iroh-blobs-0.103.1 adds chrono wasmbind + getrandom wasm_js. Browser iroh is relay-only (no UDP/mDNS/portmapper), so LAN tier is unavailable in-browser.")
//! @yah:handoff("xlb modules NOT neutral: cache.rs (std::fs, tokio Mutex/RwLock), bandwidth.rs (std::fs /proc reads), transport/blobs.rs (iroh + mshr Endpoint), transport/http.rs (reqwest rustls-tls), seed.rs (R2 upload), testing.rs. Likely neutral: verify.rs (blake3/hmac/sha2), metrics.rs, source.rs trait/progress types, retention/Ref types -- not individually verified by compile.")
//! @yah:handoff("RECOMMENDED SPLIT: (1) extract policy/Ref/verify/metrics/progress types into a dependency-light crate (or `default = [\"native\"]` feature on xlb where `native` gates cache/bandwidth/seed/testing/transport, iroh, mshr, reqwest-rustls, tokio rt-multi-thread, tls-ring); (2) wasm client enables iroh+iroh-blobs without tls-ring (relay/HTTP-edge tiers) and implements the cache trait over OPFS. No gate landed: lib.rs/cache.rs are being edited by R002-F1 and gating needs cfg on cache.rs's module; Cargo.toml/lib.rs untouched, so native tests unaffected.")
//! @yah:handoff("UNVERIFIED: whether `tls-ring` (ring 0.17.14) builds for wasm32-unknown-unknown (needs clang; not tried) and whether iroh with a real Endpoint works end-to-end in a browser; reqwest rustls-tls on wasm not checked (reqwest uses its wasm backend, TLS features are ignored/unsupported there).")
//! @yah:verify("cargo check -p xlb --target wasm32-unknown-unknown  (fails in mio; log /tmp/r002s3-1.log)")
//! @yah:verify("cargo tree -p xlb --target wasm32-unknown-unknown -i mio")
//! @yah:verify("scratch crate /tmp/r002s3-probe: iroh 1.3 + iroh-blobs 0.103 default-features=false, cargo check --target wasm32-unknown-unknown (EXIT=0; log /tmp/r002s3-2.log)")
//! @yah:verify("grep wasm in ~/.cargo/registry iroh-1.3.0/Cargo.toml and iroh-blobs-0.103.1/Cargo.toml")

pub mod bandwidth;
pub(crate) mod cache;
pub mod metrics;
pub(crate) mod resolver;
pub mod seed;
pub(crate) mod source;
pub mod testing;
pub mod transport;
pub(crate) mod verify;

pub use bandwidth::BandwidthGovernor;
pub use cache::CacheStats;
pub use metrics::{FetchObserver, FetchReport};
pub use resolver::ManifestSource;
pub use seed::{derive_key, seed_blob, R2Target, SeedOutcome};
pub(crate) use source::BlobSource;
pub use source::{FetchProgress, FetchTier, ProgressSink};
pub use xlb_core::{refs, retention};
pub use xlb_core::{BlakeHash, InvalidHash, Ref, Resolution, ResolveError};
pub use xlb_core::{RetentionIndex, RetentionPolicy};

use bytes::Bytes;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;

use crate::cache::Cache;

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid hash: {0}")]
    InvalidHash(#[from] InvalidHash),

    #[error("all sources exhausted for {0}")]
    FetchFailed(BlakeHash),

    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        expected: BlakeHash,
        actual: BlakeHash,
    },

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// ─── Policy types ─────────────────────────────────────────────────────────────

/// The role this process plays for an `AssetClass`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedRole {
    /// Fetch only; never serve bytes to remote peers.
    Passive,
    /// Fetch + seed under the class's per-tier bandwidth cap.
    Participant,
    /// Fetch + seed without caps; always-on. Used by yah-cloud permanent seeds.
    Permanent,
}

/// Coarse label used by `BandwidthPolicy` to bucket peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeerTier {
    Cloud,
    Camp,
    Workstation,
    Mobile,
}

/// Upload/download bandwidth caps for one tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BwCaps {
    pub up_mbit: u32,
    pub down_mbit: u32,
}

impl BwCaps {
    /// Passive peers fetch but never upload.
    pub fn passive() -> Self {
        Self {
            up_mbit: 0,
            down_mbit: 50,
        }
    }
}

/// Per-class bandwidth policy: caps keyed by `PeerTier`.
///
/// ```
/// use xlb::{BandwidthPolicy, BwCaps, PeerTier};
///
/// let policy = BandwidthPolicy::default()
///     .role(PeerTier::Cloud,       BwCaps { up_mbit: 1000, down_mbit: 10_000 })
///     .role(PeerTier::Camp,         BwCaps { up_mbit: 10,   down_mbit: 100 })
///     .role(PeerTier::Workstation, BwCaps { up_mbit: 5,    down_mbit: 50 })
///     .role(PeerTier::Mobile,      BwCaps::passive());
/// ```
#[derive(Debug, Clone, Default)]
pub struct BandwidthPolicy {
    roles: HashMap<PeerTier, BwCaps>,
}

impl BandwidthPolicy {
    pub fn role(mut self, tier: PeerTier, caps: BwCaps) -> Self {
        self.roles.insert(tier, caps);
        self
    }

    pub fn caps_for(&self, tier: PeerTier) -> BwCaps {
        self.roles
            .get(&tier)
            .copied()
            .unwrap_or_else(BwCaps::passive)
    }
}

// ─── Discovery ────────────────────────────────────────────────────────────────

/// Which discovery mechanisms to enable for an `AssetClass`.
///
/// xlb-2 wires in mDNS and iroh-relay discovery; xlb-1 only has Static
/// (permanent seeds resolved via the class config).
#[derive(Debug, Clone)]
pub struct Discovery {
    /// Announce and discover peers via mDNS on the local subnet.
    pub lan: bool,
    /// Announce and discover peers through iroh relay servers.
    pub swarm: bool,
    pub relays: Vec<String>,
}

impl Default for Discovery {
    fn default() -> Self {
        Self {
            lan: true,
            swarm: true,
            relays: vec![],
        }
    }
}

impl Discovery {
    pub fn with_relays(mut self, relays: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.relays = relays.into_iter().map(Into::into).collect();
        self
    }

    pub fn lan_only() -> Self {
        Self {
            lan: true,
            swarm: false,
            relays: vec![],
        }
    }

    pub fn none() -> Self {
        Self {
            lan: false,
            swarm: false,
            relays: vec![],
        }
    }
}

// ─── AssetClassConfig ─────────────────────────────────────────────────────────

/// Configuration for an `AssetClass`.
pub struct AssetClassConfig {
    /// Stable name for this class. Used as the swarm topic and log identifier.
    pub name: &'static str,
    /// Pinned `NodeId`s of always-on seed servers (string form; resolved in xlb-2).
    pub permanent_seeds: Vec<String>,
    /// URL template for the CDN fallback; `{blake3}` is substituted per-asset.
    pub cdn_fallback: Option<String>,
    pub discovery: Discovery,
    pub bandwidth: BandwidthPolicy,
    /// Base directory for the local asset cache. `None` = in-memory only.
    pub cache_dir: Option<PathBuf>,
    /// LRU eviction budget for cached assets, in bytes.
    pub cache_budget_bytes: u64,
    /// Releases before the current one whose manifest blobs stay pinned
    /// (see [`AssetClass::record_release`]). `1` = current + previous.
    pub keep_releases: usize,
    /// HTTPS URL of this class's JSON manifest (see [`refs`]). `None` = no
    /// manifest; supply one with [`AssetClass::set_manifest_source`] or use
    /// only `Ref::Hash`.
    pub manifest_url: Option<String>,
    /// How long a manifest fetch (and the `Version`/`Latest` resolutions
    /// derived from it) stays fresh before [`AssetClass::resolve`] refetches.
    pub resolve_ttl: std::time::Duration,
    /// Called once per completed fetch with a [`FetchReport`] (R423-T7).
    ///
    /// `None` — the default — means no instrumentation and no cost. Set it to
    /// measure which tier is actually serving traffic; W160's 50%-swarm-hit
    /// cost envelope is unverified until something does.
    pub observer: Option<FetchObserver>,
}

impl Default for AssetClassConfig {
    fn default() -> Self {
        Self {
            name: "default",
            permanent_seeds: vec![],
            cdn_fallback: None,
            discovery: Discovery::default(),
            bandwidth: BandwidthPolicy::default(),
            cache_dir: None,
            cache_budget_bytes: 1024 * 1024 * 1024,
            keep_releases: 1,
            manifest_url: None,
            resolve_ttl: std::time::Duration::from_secs(300),
            observer: None,
        }
    }
}

// ─── AssetClass ───────────────────────────────────────────────────────────────

struct ClassInner {
    config: AssetClassConfig,
    /// `FetchTier::Cache` — in-memory when `config.cache_dir` is `None`,
    /// disk-backed with LRU eviction (gated on `config.cache_budget_bytes`)
    /// otherwise. Disk-backed entries survive process restart.
    cache: Cache,
    /// Fetch sources. Rebuilt into a `FetchChain` at first fetch; subsequent
    /// sources added via `add_source` are appended and the chain rebuilt.
    sources: RwLock<Vec<Arc<dyn BlobSource>>>,
    role: RwLock<(SeedRole, PeerTier)>,
    /// Bandwidth governor — enforces per-tier caps and auto-governors
    /// (battery/metered). Updated via `set_governor_*` and `probe_os`.
    governor: BandwidthGovernor,
    /// Manifest, resolution memo and applied pin-set names (R002-F2).
    resolver: tokio::sync::Mutex<resolver::ResolverState>,
}

/// A registered asset class — a per-app namespace for content-addressed blobs.
///
/// `AssetClass` is `Clone + Send + Sync`; clone it freely to share across tasks.
/// Drop the last clone to stop seeding gracefully (xlb-2+).
#[derive(Clone)]
pub struct AssetClass(Arc<ClassInner>);

/// Eviction deletes are best-effort; the index has already dropped the entries.
fn log_evict_failure(r: Result<(), xlb_core::StoreError>) {
    if let Err(e) = r {
        tracing::warn!("cache eviction delete failed: {e}");
    }
}

impl AssetClass {
    /// Register a new asset class.
    ///
    /// When `config.cdn_fallback` is `Some`, an [`HttpFetcher`] is
    /// automatically wired in as the `FetchTier::Cdn` source. The iroh-blobs
    /// transport (tiers 2–4) is attached separately via
    /// [`BlobTransport::attach_fetcher`].
    ///
    /// A [`BandwidthGovernor`] is built from `config.bandwidth` and probed
    /// against the OS power source at startup (xlb-4).
    pub async fn register(config: AssetClassConfig) -> Result<Self> {
        // Pre-build CDN source before moving config into ClassInner.
        let cdn: Option<Arc<dyn BlobSource>> = if let Some(url) = &config.cdn_fallback {
            match transport::http::HttpFetcher::new(url.as_str()) {
                Ok(f) => Some(Arc::new(f)),
                Err(e) => {
                    tracing::warn!(url = %url, "HttpFetcher init failed: {e}");
                    None
                }
            }
        } else {
            None
        };

        let initial_sources: Vec<Arc<dyn BlobSource>> = cdn.into_iter().collect();

        // Build and probe the bandwidth governor.
        let governor = BandwidthGovernor::new(config.bandwidth.clone());
        governor.probe_os();

        // Build the cache layer. `cache_dir = None` → in-memory; `Some` →
        // disk-backed at the given path with LRU eviction gated on
        // `cache_budget_bytes`. The latter survives process restart, which
        // is the whole reason this layer exists (see W160 F3).
        let cache = Cache::new(
            config.cache_dir.as_deref(),
            RetentionPolicy::new(config.cache_budget_bytes, config.keep_releases),
        )
        .await?;

        let manifest_source: Option<Arc<dyn ManifestSource>> = match config
            .manifest_url
            .as_deref()
            .map(resolver::HttpManifestSource::new)
        {
            Some(Ok(s)) => Some(Arc::new(s)),
            Some(Err(e)) => {
                tracing::warn!("manifest source init failed: {e}");
                None
            }
            None => None,
        };

        Ok(Self(Arc::new(ClassInner {
            config,
            resolver: tokio::sync::Mutex::new(resolver::ResolverState {
                source: manifest_source,
                ..Default::default()
            }),
            cache,
            sources: RwLock::new(initial_sources),
            role: RwLock::new((SeedRole::Participant, PeerTier::Workstation)),
            governor,
        })))
    }

    /// Update this peer's role and tier for the class.
    pub async fn set_role(&self, role: SeedRole, tier: PeerTier) -> Result<()> {
        *self.0.role.write().await = (role, tier);
        Ok(())
    }

    /// Return a handle to a specific asset within this class.
    pub fn asset(&self, hash: BlakeHash) -> Asset {
        Asset {
            class: self.clone(),
            hash,
        }
    }

    /// The configured name of this class.
    pub fn name(&self) -> &str {
        self.0.config.name
    }

    /// Borrow the configured permanent-seed list.
    ///
    /// Each entry is a hex-encoded `NodeId`; parse with
    /// [`transport::blobs::parse_seed_node_id`]. Used by
    /// [`transport::BlobTransport::attach_permanent_seeds`] to wire
    /// `FetchTier::Seed` sources at boot.
    pub(crate) fn permanent_seeds(&self) -> &[String] {
        &self.0.config.permanent_seeds
    }

    /// Borrow the bandwidth governor for this class.
    ///
    /// Use [`BandwidthGovernor::effective_caps`] to query per-tier caps
    /// (already adjusted for battery/metered state).
    pub fn governor(&self) -> &BandwidthGovernor {
        &self.0.governor
    }

    /// Notify the governor that battery state changed.
    ///
    /// Call from OS power-event hooks (e.g. `NSWorkspaceDidChangeNotification`
    /// on macOS) to keep caps updated in real time.
    pub fn set_battery(&self, on_battery: bool) {
        self.0.governor.set_battery(on_battery);
    }

    /// Notify the governor that metered-connection state changed.
    pub fn set_metered(&self, metered: bool) {
        self.0.governor.set_metered(metered);
    }

    /// Current cache occupancy for this class (R423-T6).
    ///
    /// Reads an in-memory index; no filesystem access, so it is safe to poll
    /// from an operator surface on a timer. See [`CacheStats::disk_backed`]
    /// before drawing a used-vs-budget ratio.
    pub async fn cache_stats(&self) -> CacheStats {
        self.0.cache.stats().await
    }

    /// Pin one blob: never evicted, outside the byte budget. May be called
    /// before the blob has been fetched.
    pub async fn pin(&self, hash: BlakeHash) {
        self.0.cache.retention().pin(hash);
    }

    /// Drop a hash pin; the blob rejoins the LRU (and may be evicted at once
    /// if the unpinned footprint is over budget).
    pub async fn unpin(&self, hash: &BlakeHash) {
        let r = self.0.cache.retention().unpin(hash).await;
        log_evict_failure(r);
    }

    /// Define or replace a named pin set (e.g. a consumer's required-asset
    /// list).
    pub async fn pin_set(&self, name: &str, hashes: impl IntoIterator<Item = BlakeHash>) {
        self.0.cache.retention().pin_set(name, hashes);
    }

    /// Remove a named pin set.
    pub async fn unpin_set(&self, name: &str) {
        let r = self.0.cache.retention().unpin_set(name).await;
        log_evict_failure(r);
    }

    /// Record `release` as current with the blobs its manifest lists; releases
    /// more than `keep_releases` back lose their protection.
    pub async fn record_release(&self, release: &str, hashes: impl IntoIterator<Item = BlakeHash>) {
        let r = self
            .0
            .cache
            .retention()
            .record_release(release, hashes)
            .await;
        log_evict_failure(r);
    }

    /// Whether `hash` is currently protected from eviction.
    pub async fn is_pinned(&self, hash: &BlakeHash) -> bool {
        self.0.cache.retention().is_pinned(hash)
    }

    /// Add a fetch source to the chain (e.g. a transport adapter or mock peer).
    ///
    /// Sources are tried in `FetchTier` priority order regardless of insertion
    /// order. Safe to call after the class is registered.
    /// Replace where this class's manifest comes from (default: HTTP GET of
    /// `config.manifest_url`). Drops any cached manifest and resolutions.
    pub async fn set_manifest_source(&self, source: Arc<dyn ManifestSource>) {
        let mut st = self.0.resolver.lock().await;
        st.source = Some(source);
        st.manifest = None;
        st.memo.clear();
    }

    /// Resolve `r` to a content hash through the class manifest.
    ///
    /// `Hash` refs return immediately and never touch the manifest.
    /// `Version`/`Latest` are memoised and re-resolved once
    /// `config.resolve_ttl` has elapsed; if the refetch fails and a manifest
    /// is already cached, the stale resolution is served (and stays expired,
    /// so the next call retries) rather than failing an offline client.
    pub async fn resolve(&self, r: &Ref) -> std::result::Result<BlakeHash, ResolveError> {
        self.resolve_at(r, resolver::now_ms()).await
    }

    /// Resolve `r` and return the [`Asset`] handle for the result.
    pub async fn resolve_asset(&self, r: &Ref) -> std::result::Result<Asset, ResolveError> {
        Ok(self.asset(self.resolve(r).await?))
    }

    /// Refetch the manifest now regardless of TTL, then re-derive pins.
    pub async fn refresh_manifest(&self) -> std::result::Result<(), ResolveError> {
        let mut st = self.0.resolver.lock().await;
        self.refresh_locked(&mut st, resolver::now_ms()).await
    }

    pub(crate) async fn resolve_at(
        &self,
        r: &Ref,
        now_ms: u64,
    ) -> std::result::Result<BlakeHash, ResolveError> {
        if let Ref::Hash(h) = r {
            return Ok(*h);
        }
        let ttl_ms = self.0.config.resolve_ttl.as_millis() as u64;
        let mut st = self.0.resolver.lock().await;
        if let Some(hit) = st.memo.get(r) {
            if !hit.is_expired(now_ms, ttl_ms) {
                return Ok(hit.hash);
            }
        }
        if st.manifest_stale(now_ms, ttl_ms) {
            if let Err(e) = self.refresh_locked(&mut st, now_ms).await {
                if st.manifest.is_none() {
                    return Err(e);
                }
                tracing::warn!(
                    class = self.name(),
                    "manifest refresh failed, using stale: {e}"
                );
            }
        }
        let (manifest, fetched_at_ms) = st.manifest.clone().ok_or(ResolveError::NoManifest)?;
        let hash = manifest.resolve(r)?;
        st.memo.insert(
            r.clone(),
            Resolution {
                hash,
                fetched_at_ms,
            },
        );
        Ok(hash)
    }

    /// Fetch + install a manifest. Pins are re-derived from it so they
    /// survive a process restart: the current release feeds
    /// [`record_release`](Self::record_release), each pinned set feeds
    /// [`pin_set`](Self::pin_set), and a set the previous manifest declared
    /// but this one dropped is unpinned.
    async fn refresh_locked(
        &self,
        st: &mut resolver::ResolverState,
        now_ms: u64,
    ) -> std::result::Result<(), ResolveError> {
        let manifest = st.fetch_manifest().await?;
        if let Some((id, hashes)) = manifest.release() {
            self.record_release(id, hashes.iter().copied()).await;
        }
        let mut applied = std::collections::HashSet::new();
        for (name, hashes) in manifest.pinned_sets() {
            self.pin_set(name, hashes.iter().copied()).await;
            applied.insert(name.to_string());
        }
        for gone in st.applied_sets.difference(&applied) {
            self.unpin_set(gone).await;
        }
        st.applied_sets = applied;
        st.manifest = Some((Arc::new(manifest), now_ms));
        st.memo.clear();
        Ok(())
    }

    pub(crate) fn add_source(&self, source: Arc<dyn BlobSource>) {
        if let Ok(mut sources) = self.0.sources.try_write() {
            sources.push(source);
        } else {
            tracing::warn!(
                class = self.name(),
                "add_source: lock contended, source dropped"
            );
        }
    }

    /// Internal: run the fetch chain for `hash`.
    pub(crate) async fn fetch_bytes(&self, hash: BlakeHash) -> Result<Bytes> {
        self.fetch_bytes_with_progress(hash, None).await
    }

    /// Internal: run the fetch chain and hand back the metric alongside the
    /// bytes (R423-T7). The observer, if any, has already been called.
    pub(crate) async fn fetch_bytes_reported(
        &self,
        hash: BlakeHash,
        sink: Option<ProgressSink>,
    ) -> (Result<Bytes>, FetchReport) {
        let started = std::time::Instant::now();

        // 1. Local cache — fast path. Disk-backed cache re-verifies BLAKE3
        //    on read; mem-backed is a HashMap lookup.
        if let Some(bytes) = self.0.cache.get(&hash).await {
            tracing::trace!(%hash, "cache hit");
            let report = self.report(
                hash,
                Some(FetchTier::Cache),
                None,
                bytes.len() as u64,
                started,
            );
            return (Ok(bytes), report);
        }

        // 2. Clone source handles so we can release the lock before awaiting.
        let sources: Vec<Arc<dyn BlobSource>> = self.0.sources.read().await.clone();

        let chain = source::FetchChain::new(sources);
        match chain.fetch_with_progress(&hash, sink.as_ref()).await {
            Some(hit) => {
                // Cache write failures are non-fatal: the fetched bytes are
                // verified, so we can still return them. A disk-full error
                // means future restarts won't have this entry, but the current
                // request succeeds.
                if let Err(e) = self.0.cache.put(hash, hit.bytes.clone()).await {
                    tracing::warn!(%hash, "cache write failed: {e}");
                }
                let report = self.report(
                    hash,
                    Some(hit.tier),
                    hit.peer_id,
                    hit.bytes.len() as u64,
                    started,
                );
                (Ok(hit.bytes), report)
            }
            // A MISS IS REPORTED, not swallowed. A hit rate computed only over
            // successful fetches has no denominator.
            None => {
                let report = self.report(hash, None, None, 0, started);
                (Err(Error::FetchFailed(hash)), report)
            }
        }
    }

    /// Build a [`FetchReport`] and hand it to the class observer.
    fn report(
        &self,
        hash: BlakeHash,
        tier: Option<FetchTier>,
        peer_id: Option<String>,
        bytes_served: u64,
        started: std::time::Instant,
    ) -> FetchReport {
        let report = FetchReport {
            class: self.0.config.name,
            hash,
            tier,
            peer_id,
            bytes_served,
            duration_ms: started.elapsed().as_millis() as u64,
        };
        if let Some(observer) = &self.0.config.observer {
            observer(&report);
        }
        report
    }

    /// Internal: run the fetch chain for `hash`, reporting byte progress.
    ///
    /// Identical to [`fetch_bytes`](Self::fetch_bytes) but threads `sink`
    /// down to the serving source. A cache hit returns without ever invoking
    /// `sink` (instant, no transfer to surface).
    pub(crate) async fn fetch_bytes_with_progress(
        &self,
        hash: BlakeHash,
        sink: Option<ProgressSink>,
    ) -> Result<Bytes> {
        // Delegates rather than duplicating the chain walk: one code path means
        // the observer cannot be live on some entry points and dead on others,
        // which is exactly how instrumentation ends up reporting a partial
        // hit rate that nobody knows is partial.
        self.fetch_bytes_reported(hash, sink).await.0
    }
}

// ─── Asset ────────────────────────────────────────────────────────────────────

/// A handle to a specific content-addressed blob within an `AssetClass`.
#[derive(Clone)]
pub struct Asset {
    class: AssetClass,
    hash: BlakeHash,
}

impl Asset {
    /// Fetch the blob, trying all sources in priority order and verifying BLAKE3.
    ///
    /// Subsequent calls return the cached copy without hitting any source.
    pub async fn fetch(&self) -> Result<Bytes> {
        self.class.fetch_bytes(self.hash).await
    }

    /// Fetch the blob, reporting incremental byte progress to `sink`.
    ///
    /// Identical to [`fetch`](Self::fetch) but the CDN tier emits per-chunk
    /// [`FetchProgress`] against `Content-Length`. Instant tiers (cache)
    /// return without ever invoking `sink`.
    pub async fn fetch_with_progress(&self, sink: ProgressSink) -> Result<Bytes> {
        self.class
            .fetch_bytes_with_progress(self.hash, Some(sink))
            .await
    }

    /// Fetch the blob and return the [`FetchReport`] alongside it (R423-T7).
    ///
    /// Identical work to [`fetch`](Self::fetch); the difference is that the
    /// caller gets to see which tier served it, from which peer, and how long
    /// the whole chain walk took. A caller that just wants bytes should keep
    /// using `fetch`.
    ///
    /// The report is produced — and the class observer called — on a miss too,
    /// with `tier: None`; the `Result` is the failure and the report is the
    /// record of it.
    pub async fn fetch_reported(&self) -> (Result<Bytes>, FetchReport) {
        self.class.fetch_bytes_reported(self.hash, None).await
    }

    /// Returns `true` if this blob is in the local cache (in-memory or
    /// disk-backed). Disk-backed `is_cached` is a presence check against the
    /// LRU index — it does *not* re-verify BLAKE3. Use [`Asset::fetch`] when
    /// you need verified bytes.
    pub async fn is_cached(&self) -> bool {
        self.class.0.cache.contains(&self.hash).await
    }

    pub fn hash(&self) -> BlakeHash {
        self.hash
    }
}
