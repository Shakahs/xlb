//! Native side of [`Ref`] resolution: fetching the per-class manifest, the
//! TTL'd resolution memo, and re-deriving pins from the manifest.
//!
//! All decisions (what matches, when something is stale) live in the pure
//! [`crate::refs`] module; this file only supplies a clock, a transport and a
//! lock.
//!
//! The manifest is a mutable pointer, not content-addressed, so its only
//! integrity is the transport's (TLS for the HTTP source). Blobs it names are
//! still BLAKE3-verified when fetched.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;

use crate::refs::{is_expired, ParsedManifest, Ref, Resolution, ResolveError};

/// Where a class's manifest bytes come from.
#[async_trait]
pub trait ManifestSource: Send + Sync {
    /// Fetch the current manifest JSON.
    async fn fetch(&self) -> Result<Vec<u8>, String>;
}

/// Manifest over plain HTTPS GET of a fixed URL.
pub(crate) struct HttpManifestSource {
    url: String,
    client: reqwest::Client,
}

impl HttpManifestSource {
    pub fn new(url: impl Into<String>) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            url: url.into(),
            client,
        })
    }
}

#[async_trait]
impl ManifestSource for HttpManifestSource {
    async fn fetch(&self) -> Result<Vec<u8>, String> {
        let resp = self
            .client
            .get(&self.url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| e.to_string())?;
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| e.to_string())
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Default)]
pub(crate) struct ResolverState {
    pub source: Option<Arc<dyn ManifestSource>>,
    pub manifest: Option<(Arc<ParsedManifest>, u64)>,
    /// Memoised Version/Latest resolutions; cleared on every manifest refresh.
    pub memo: HashMap<Ref, Resolution>,
    /// Pin-set names last applied from the manifest, so a set the manifest
    /// drops is unpinned on the next refresh.
    pub applied_sets: HashSet<String>,
}

impl ResolverState {
    pub fn manifest_stale(&self, now_ms: u64, ttl_ms: u64) -> bool {
        match &self.manifest {
            None => true,
            Some((_, at)) => is_expired(*at, now_ms, ttl_ms),
        }
    }

    pub async fn fetch_manifest(&self) -> Result<ParsedManifest, ResolveError> {
        let src = self.source.as_ref().ok_or(ResolveError::NoManifest)?;
        let bytes = src.fetch().await.map_err(ResolveError::Fetch)?;
        ParsedManifest::from_json(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use semver::VersionReq;

    use super::*;
    use crate::{refs::MANIFEST_SCHEMA, AssetClass, AssetClassConfig, BlakeHash};

    fn h(n: u8) -> BlakeHash {
        BlakeHash::from_bytes([n; 32])
    }

    fn json(latest: u8, pinned: &[(&str, u8)]) -> Vec<u8> {
        let sets: Vec<String> = pinned
            .iter()
            .map(|(n, b)| format!(r#""{n}":["{}"]"#, h(*b).to_hex()))
            .collect();
        format!(
            r#"{{"schema":{MANIFEST_SCHEMA},
              "release":{{"id":"r1","hashes":["{}"]}},
              "pinned_sets":{{{}}},
              "entries":[{{"name":"a","version":"1.0.0","channels":["stable"],"hash":"{}","size":1}}]}}"#,
            h(latest).to_hex(),
            sets.join(","),
            h(latest).to_hex(),
        )
        .into_bytes()
    }

    struct Fake {
        body: std::sync::Mutex<Result<Vec<u8>, String>>,
        calls: AtomicUsize,
    }

    impl Fake {
        fn new(body: Result<Vec<u8>, String>) -> Arc<Self> {
            Arc::new(Self {
                body: std::sync::Mutex::new(body),
                calls: AtomicUsize::new(0),
            })
        }
        fn set(&self, b: Result<Vec<u8>, String>) {
            *self.body.lock().unwrap() = b;
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ManifestSource for Fake {
        async fn fetch(&self) -> Result<Vec<u8>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.body.lock().unwrap().clone()
        }
    }

    async fn class(src: Arc<Fake>) -> AssetClass {
        let c = AssetClass::register(AssetClassConfig {
            resolve_ttl: Duration::from_millis(1000),
            ..Default::default()
        })
        .await
        .unwrap();
        c.set_manifest_source(src).await;
        c
    }

    #[tokio::test]
    async fn hash_ref_never_fetches() {
        let src = Fake::new(Ok(json(1, &[])));
        let c = class(src.clone()).await;
        assert_eq!(c.resolve_at(&Ref::Hash(h(7)), 0).await.unwrap(), h(7));
        assert_eq!(
            c.resolve_at(&Ref::Hash(h(7)), u64::MAX).await.unwrap(),
            h(7)
        );
        assert_eq!(src.calls(), 0);
    }

    #[tokio::test]
    async fn version_and_latest_resolve_and_reresolve_on_ttl() {
        let src = Fake::new(Ok(json(1, &[])));
        let c = class(src.clone()).await;
        let v = Ref::Version("a".into(), VersionReq::parse("^1").unwrap());
        let l = Ref::Latest("a".into(), "stable".into());
        assert_eq!(c.resolve_at(&v, 100).await.unwrap(), h(1));
        assert_eq!(c.resolve_at(&l, 200).await.unwrap(), h(1));
        assert_eq!(src.calls(), 1); // one manifest fetch serves both
                                    // Manifest changes upstream; within TTL the memo still answers.
        src.set(Ok(json(2, &[])));
        assert_eq!(c.resolve_at(&v, 900).await.unwrap(), h(1));
        assert_eq!(src.calls(), 1);
        // Past TTL: refetch and re-resolve.
        assert_eq!(c.resolve_at(&v, 1100).await.unwrap(), h(2));
        assert_eq!(c.resolve_at(&l, 1100).await.unwrap(), h(2));
        assert_eq!(src.calls(), 2);
    }

    #[tokio::test]
    async fn stale_served_when_refetch_fails_but_cold_errors() {
        let src = Fake::new(Ok(json(1, &[])));
        let c = class(src.clone()).await;
        let l = Ref::Latest("a".into(), "stable".into());
        assert_eq!(c.resolve_at(&l, 0).await.unwrap(), h(1));
        src.set(Err("offline".into()));
        assert_eq!(c.resolve_at(&l, 5000).await.unwrap(), h(1));
        assert_eq!(c.resolve_at(&l, 5001).await.unwrap(), h(1));
        assert_eq!(src.calls(), 3); // each stale call retries

        let cold = class(Fake::new(Err("offline".into()))).await;
        assert!(matches!(
            cold.resolve_at(&l, 0).await,
            Err(ResolveError::Fetch(_))
        ));
    }

    #[tokio::test]
    async fn no_source_is_no_manifest() {
        let c = AssetClass::register(AssetClassConfig::default())
            .await
            .unwrap();
        let l = Ref::Latest("a".into(), "stable".into());
        assert_eq!(c.resolve_at(&l, 0).await, Err(ResolveError::NoManifest));
    }

    #[tokio::test]
    async fn pins_rederived_from_manifest_and_dropped_sets_unpinned() {
        let src = Fake::new(Ok(json(1, &[("boot", 5)])));
        let c = class(src.clone()).await;
        assert!(!c.is_pinned(&h(1)).await);
        assert!(!c.is_pinned(&h(5)).await);
        c.refresh_manifest().await.unwrap();
        assert!(c.is_pinned(&h(1)).await, "release hash");
        assert!(c.is_pinned(&h(5)).await, "pinned set hash");
        // Next manifest drops the set and rewrites the release.
        src.set(Ok(json(2, &[])));
        c.refresh_manifest().await.unwrap();
        assert!(!c.is_pinned(&h(5)).await);
        assert!(c.is_pinned(&h(2)).await);
    }
}
