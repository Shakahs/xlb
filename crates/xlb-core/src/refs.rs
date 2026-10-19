//! Platform-neutral asset references and per-class manifest resolution.
//!
//! A [`Ref`] names an asset three ways:
//!
//! - [`Ref::Hash`] — a content hash. Immutable: it identifies its bytes and is
//!   never re-resolved.
//! - [`Ref::Version`] — `(name, semver requirement)`; resolves to the highest
//!   matching entry in the class manifest.
//! - [`Ref::Latest`] — `(name, channel)`; resolves to the highest-versioned
//!   entry of that name published on the channel.
//!
//! Like [`crate::retention`], this module is pure — no filesystem, no tokio,
//! no clocks (callers pass `now` in milliseconds) — so the browser core
//! (R002-F4) can reuse it unchanged on wasm32. Fetching the manifest bytes is
//! the native side's job (`AssetClass::resolve`).
//!
//! ## Manifest format
//!
//! JSON with a `schema` field ([`MANIFEST_SCHEMA`]):
//!
//! ```json
//! { "schema": 1,
//!   "release": { "id": "1.4.0", "hashes": ["<hex>", ...] },
//!   "pinned_sets": { "boot": ["<hex>", ...] },
//!   "entries": [
//!     { "name": "hrtf", "version": "1.2.0", "channels": ["stable"],
//!       "hash": "<hex>", "size": 123 } ] }
//! ```

use std::collections::BTreeMap;

use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};

use crate::hash::BlakeHash;

/// The only manifest `schema` this build understands.
pub const MANIFEST_SCHEMA: u32 = 1;

/// A reference to an asset in a class.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Ref {
    /// Immutable content address.
    Hash(BlakeHash),
    /// Highest manifest version of `name` satisfying the requirement.
    Version(String, VersionReq),
    /// Highest manifest version of `name` published on `channel`.
    Latest(String, String),
}

impl Ref {
    /// Hash refs never change meaning; Version/Latest are mutable pointers.
    pub fn is_immutable(&self) -> bool {
        matches!(self, Ref::Hash(_))
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResolveError {
    #[error("manifest parse: {0}")]
    Parse(String),
    #[error("unsupported manifest schema {0} (want {MANIFEST_SCHEMA})")]
    Schema(u32),
    #[error("manifest entry {name:?}: {reason}")]
    Entry { name: String, reason: String },
    #[error("no manifest entry matches {0}")]
    NotFound(String),
    #[error("no manifest source configured and no cached manifest")]
    NoManifest,
    #[error("manifest fetch failed: {0}")]
    Fetch(String),
}

/// One published asset version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub name: String,
    /// Semver string.
    pub version: String,
    #[serde(default)]
    pub channels: Vec<String>,
    /// Lowercase hex blake3.
    pub hash: String,
    pub size: u64,
}

/// The current release: a label plus the blobs it consists of.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ReleaseSet {
    pub id: String,
    #[serde(default)]
    pub hashes: Vec<String>,
}

/// Wire form of a class manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub schema: u32,
    #[serde(default)]
    pub release: Option<ReleaseSet>,
    #[serde(default)]
    pub pinned_sets: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub entries: Vec<ManifestEntry>,
}

/// A validated manifest: every version and hash parsed up front, so
/// resolution itself cannot fail on malformed data.
#[derive(Debug, Clone)]
pub struct ParsedManifest {
    manifest: Manifest,
    parsed: Vec<(Version, BlakeHash)>,
    release: Option<(String, Vec<BlakeHash>)>,
    pinned_sets: Vec<(String, Vec<BlakeHash>)>,
}

fn hash_of(name: &str, s: &str) -> Result<BlakeHash, ResolveError> {
    BlakeHash::from_hex(s).map_err(|e| ResolveError::Entry {
        name: name.to_string(),
        reason: e.to_string(),
    })
}

impl ParsedManifest {
    pub fn from_json(bytes: &[u8]) -> Result<Self, ResolveError> {
        let m: Manifest =
            serde_json::from_slice(bytes).map_err(|e| ResolveError::Parse(e.to_string()))?;
        Self::new(m)
    }

    pub fn new(manifest: Manifest) -> Result<Self, ResolveError> {
        if manifest.schema != MANIFEST_SCHEMA {
            return Err(ResolveError::Schema(manifest.schema));
        }
        let mut parsed = Vec::with_capacity(manifest.entries.len());
        for e in &manifest.entries {
            let version = Version::parse(&e.version).map_err(|err| ResolveError::Entry {
                name: e.name.clone(),
                reason: format!("version {:?}: {err}", e.version),
            })?;
            parsed.push((version, hash_of(&e.name, &e.hash)?));
        }
        let release = match &manifest.release {
            Some(r) => Some((
                r.id.clone(),
                r.hashes
                    .iter()
                    .map(|h| hash_of(&r.id, h))
                    .collect::<Result<_, _>>()?,
            )),
            None => None,
        };
        let mut pinned_sets = Vec::new();
        for (name, hs) in &manifest.pinned_sets {
            pinned_sets.push((
                name.clone(),
                hs.iter()
                    .map(|h| hash_of(name, h))
                    .collect::<Result<_, _>>()?,
            ));
        }
        Ok(Self {
            manifest,
            parsed,
            release,
            pinned_sets,
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The current release (id + blobs), if the manifest declares one.
    pub fn release(&self) -> Option<(&str, &[BlakeHash])> {
        self.release.as_ref().map(|(id, hs)| (id.as_str(), &hs[..]))
    }

    /// Named pin sets declared by the manifest.
    pub fn pinned_sets(&self) -> impl Iterator<Item = (&str, &[BlakeHash])> {
        self.pinned_sets.iter().map(|(n, h)| (n.as_str(), &h[..]))
    }

    /// Resolve `r` to a content hash. `Hash` refs return themselves without
    /// consulting the manifest.
    pub fn resolve(&self, r: &Ref) -> Result<BlakeHash, ResolveError> {
        let best = |pred: &dyn Fn(&ManifestEntry, &Version) -> bool| {
            self.manifest
                .entries
                .iter()
                .zip(&self.parsed)
                .filter(|(e, (v, _))| pred(e, v))
                .max_by(|a, b| a.1 .0.cmp(&b.1 .0))
                .map(|(_, (_, h))| *h)
        };
        match r {
            Ref::Hash(h) => Ok(*h),
            Ref::Version(name, req) => best(&|e, v| e.name == *name && req.matches(v))
                .ok_or_else(|| ResolveError::NotFound(format!("{name} {req}"))),
            Ref::Latest(name, channel) => {
                best(&|e, _| e.name == *name && e.channels.iter().any(|c| c == channel))
                    .ok_or_else(|| ResolveError::NotFound(format!("{name}@{channel}")))
            }
        }
    }
}

/// A cached resolution and when its manifest was fetched (ms since epoch,
/// caller's clock).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolution {
    pub hash: BlakeHash,
    pub fetched_at_ms: u64,
}

/// Whether something fetched at `fetched_at_ms` is stale at `now_ms`. A clock
/// that moved backwards counts as fresh rather than stale-forever.
pub fn is_expired(fetched_at_ms: u64, now_ms: u64, ttl_ms: u64) -> bool {
    now_ms.saturating_sub(fetched_at_ms) >= ttl_ms
}

impl Resolution {
    /// Whether this resolution must be re-resolved at `now_ms`. Hash refs are
    /// immutable, so callers never expire them; for the rest, an entry is
    /// stale once `ttl_ms` has elapsed.
    pub fn is_expired(&self, now_ms: u64, ttl_ms: u64) -> bool {
        is_expired(self.fetched_at_ms, now_ms, ttl_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u8) -> BlakeHash {
        BlakeHash::from_bytes([n; 32])
    }

    fn entry(name: &str, v: &str, ch: &[&str], n: u8) -> ManifestEntry {
        ManifestEntry {
            name: name.into(),
            version: v.into(),
            channels: ch.iter().map(|s| s.to_string()).collect(),
            hash: h(n).to_hex(),
            size: 1,
        }
    }

    fn manifest() -> ParsedManifest {
        ParsedManifest::new(Manifest {
            schema: MANIFEST_SCHEMA,
            release: Some(ReleaseSet {
                id: "r2".into(),
                hashes: vec![h(3).to_hex()],
            }),
            pinned_sets: BTreeMap::from([("boot".into(), vec![h(1).to_hex()])]),
            entries: vec![
                entry("hrtf", "1.0.0", &["stable"], 1),
                entry("hrtf", "1.2.0", &["stable"], 2),
                entry("hrtf", "2.0.0", &["beta"], 3),
                entry("font", "0.1.0", &["stable"], 4),
            ],
        })
        .unwrap()
    }

    #[test]
    fn hash_ref_resolves_to_itself() {
        let m = ParsedManifest::new(Manifest {
            schema: MANIFEST_SCHEMA,
            release: None,
            pinned_sets: BTreeMap::new(),
            entries: vec![],
        })
        .unwrap();
        assert_eq!(m.resolve(&Ref::Hash(h(9))).unwrap(), h(9));
        assert!(Ref::Hash(h(9)).is_immutable());
    }

    #[test]
    fn version_ref_picks_highest_match() {
        let m = manifest();
        let req = |s: &str| Ref::Version("hrtf".into(), VersionReq::parse(s).unwrap());
        assert_eq!(m.resolve(&req("^1")).unwrap(), h(2));
        assert_eq!(m.resolve(&req("=1.0.0")).unwrap(), h(1));
        assert_eq!(m.resolve(&req(">=1, <3")).unwrap(), h(3));
        assert!(matches!(
            m.resolve(&req("^9")),
            Err(ResolveError::NotFound(_))
        ));
        assert!(!req("^1").is_immutable());
    }

    #[test]
    fn latest_ref_filters_by_channel() {
        let m = manifest();
        let l = |c: &str| Ref::Latest("hrtf".into(), c.into());
        assert_eq!(m.resolve(&l("stable")).unwrap(), h(2));
        assert_eq!(m.resolve(&l("beta")).unwrap(), h(3));
        assert!(m.resolve(&l("nightly")).is_err());
        assert!(m
            .resolve(&Ref::Latest("nope".into(), "stable".into()))
            .is_err());
    }

    #[test]
    fn ttl_expiry_takes_now_as_parameter() {
        let r = Resolution {
            hash: h(1),
            fetched_at_ms: 1_000,
        };
        assert!(!r.is_expired(1_999, 1_000));
        assert!(r.is_expired(2_000, 1_000));
        assert!(!r.is_expired(500, 1_000)); // clock went backwards
    }

    #[test]
    fn json_round_trip_and_validation() {
        let m = manifest();
        let json = serde_json::to_vec(m.manifest()).unwrap();
        let back = ParsedManifest::from_json(&json).unwrap();
        assert_eq!(back.manifest(), m.manifest());
        assert_eq!(back.release().unwrap().0, "r2");
        assert_eq!(back.pinned_sets().count(), 1);

        let bad_schema = br#"{"schema":2}"#;
        assert_eq!(
            ParsedManifest::from_json(bad_schema).unwrap_err(),
            ResolveError::Schema(2)
        );
        let bad_ver = format!(
            r#"{{"schema":1,"entries":[{{"name":"a","version":"x","hash":"{}","size":1}}]}}"#,
            h(1).to_hex()
        );
        assert!(matches!(
            ParsedManifest::from_json(bad_ver.as_bytes()),
            Err(ResolveError::Entry { .. })
        ));
        assert!(ParsedManifest::from_json(b"nope").is_err());
    }
}
