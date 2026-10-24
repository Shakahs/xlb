//! R1003 — pinned trees: durable named pin sets, seeded from local paths,
//! served to peers by a long-lived node.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use mshr::{Discovery, Endpoint, Keypair};
use xlb::{
    pinned::hashes_in_dir, transport::BlobTransport, AssetClass, AssetClassConfig, BlakeHash,
    FetchTier,
};

fn tmpdir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "xlb-r1003-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// A CAS checkout: `n` blobs of `len` bytes at `<dir>/<blake3-hex>`.
fn checkout(dir: &Path, n: u8, len: usize) -> Vec<BlakeHash> {
    (0..n)
        .map(|i| {
            let data = vec![i.wrapping_mul(37).wrapping_add(1); len];
            let h = BlakeHash::hash(&data);
            std::fs::write(dir.join(h.to_hex()), &data).unwrap();
            h
        })
        .collect()
}

async fn disk_class(cache: &Path, budget: u64) -> AssetClass {
    AssetClass::register(AssetClassConfig {
        name: "r1003",
        cache_dir: Some(cache.to_path_buf()),
        cache_budget_bytes: budget,
        ..Default::default()
    })
    .await
    .expect("register")
}

#[tokio::test]
async fn a_pinned_tree_survives_restart_and_budget_pressure() {
    let src = tmpdir("src");
    let cache = tmpdir("cache");
    // 3 KB of pinned tree against a 500-byte budget: the tree alone is six
    // times over, so any eviction path that ignores pins would show here.
    let tree = checkout(&src, 3, 1000);
    assert_eq!(hashes_in_dir(&src).unwrap().len(), 3);

    {
        let class = disk_class(&cache, 500).await;
        let info = class.pin_tree("keys-pack", hashes_in_dir(&src).unwrap()).await.unwrap();
        assert_eq!((info.members, info.present, info.durable), (3, 0, true));
        let r = class.seed_tree_from_dirs("keys-pack", &[src.clone()]).await.unwrap();
        assert_eq!(r.ingested, 3);
        assert!(r.missing.is_empty() && r.mismatched.is_empty());
    }

    // Restart: a fresh register over the same cache_dir. Before R1003 the
    // replay ran with no pins and evicted the tree down to the budget.
    let class = disk_class(&cache, 500).await;
    let trees = class.pinned_trees();
    assert_eq!(trees.len(), 1, "tree reloaded from <cache_dir>/pins");
    assert_eq!((trees[0].present, trees[0].present_bytes), (3, 3000));

    // Pressure: 20 unpinned 400-byte blobs through a 500-byte budget.
    for i in 0..20u32 {
        class.ingest(Bytes::from(vec![(i % 251) as u8 + 3; 400])).await.unwrap();
    }
    for h in &tree {
        assert!(class.asset(*h).is_cached().await, "{h} evicted");
        assert!(class.is_pinned(h).await);
    }
    let stats = class.cache_stats().await;
    assert!(stats.bytes - stats.pinned_bytes <= 500, "budget still enforced on the unpinned share");

    // A second seed pass re-reads nothing.
    let r = class.seed_tree_from_dirs("keys-pack", &[src]).await.unwrap();
    assert_eq!((r.ingested, r.already_cached), (0, 3));
}

#[tokio::test]
async fn unpinning_a_tree_is_durable_and_frees_its_blobs() {
    let src = tmpdir("src-un");
    let cache = tmpdir("cache-un");
    checkout(&src, 3, 1000);
    {
        let class = disk_class(&cache, 500).await;
        class.pin_tree("t", hashes_in_dir(&src).unwrap()).await.unwrap();
        class.seed_tree_from_dirs("t", &[src]).await.unwrap();
        class.unpin_tree("t").await.unwrap();
        assert!(class.cache_stats().await.bytes <= 500, "over-budget members evicted on unpin");
        assert!(class.unpin_tree("t").await.is_err(), "second unpin names the missing tree");
    }
    let class = disk_class(&cache, 500).await;
    assert!(class.pinned_trees().is_empty(), "removal persisted");
}

#[tokio::test]
async fn seeding_skips_corrupt_files_and_reports_missing_members() {
    let src = tmpdir("src-bad");
    let cache = tmpdir("cache-bad");
    let good = checkout(&src, 1, 64);
    // A file named for one hash holding other bytes.
    let liar = BlakeHash::hash(b"what the name promises");
    std::fs::write(src.join(liar.to_hex()), b"something else").unwrap();
    let absent = BlakeHash::hash(b"nobody has this");

    let class = disk_class(&cache, 1 << 20).await;
    class.pin_tree("mixed", [good[0], liar, absent]).await.unwrap();
    let r = class.seed_tree_from_dirs("mixed", &[src.clone()]).await.unwrap();
    assert_eq!(r.ingested, 1);
    assert_eq!(r.mismatched, vec![src.join(liar.to_hex())]);
    // Neither the liar nor the absent hash is held.
    let mut missing = r.missing.clone();
    missing.sort_by_key(BlakeHash::to_hex);
    let mut want = vec![liar, absent];
    want.sort_by_key(BlakeHash::to_hex);
    assert_eq!(missing, want);
    assert!(!class.asset(liar).is_cached().await);
}

#[tokio::test]
async fn invalid_tree_names_are_refused() {
    let class = disk_class(&tmpdir("names"), 1 << 20).await;
    assert!(class.pin_tree("../escape", []).await.is_err());
}

/// The camp-node shape end to end: a node seeds a tree from a checkout and
/// publishes it; a client whose CDN is unreachable fetches a member from the
/// node at `FetchTier::Seed`. Reached here by a static address on loopback;
/// the LAN-discovered variant is `..._over_lan_discovery` below.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_fetches_a_pinned_blob_from_the_node_with_the_cdn_down() {
    let src = tmpdir("src-net");
    let tree = checkout(&src, 2, 4096);

    let node_ep = Endpoint::builder().keypair(Keypair::generate()).bind().await.unwrap();
    let node = BlobTransport::new(node_ep).await.unwrap();
    let node_class = disk_class(&tmpdir("cache-net"), 1 << 20).await;
    node_class.pin_tree("pack", tree.clone()).await.unwrap();
    node_class.seed_tree_from_dirs("pack", &[src]).await.unwrap();
    assert_eq!(node.serve_pinned_trees(&node_class).await.unwrap(), 2);

    let node_addr = node.node_addr();
    let client_ep = Endpoint::builder()
        .keypair(Keypair::generate())
        .discovery(Discovery::new().with_static([node_addr.clone()]))
        .bind()
        .await
        .unwrap();
    let client = BlobTransport::new(client_ep).await.unwrap();
    let client_class = AssetClass::register(AssetClassConfig {
        name: "r1003-client",
        // Port 9 (discard) on loopback: refused immediately, i.e. "CDN down".
        cdn_fallback: Some("http://127.0.0.1:9/{blake3}".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    client.attach_fetcher(&client_class, node_addr, FetchTier::Seed);

    let (got, report) = client_class.asset(tree[1]).fetch_reported().await;
    let got = got.expect("fetched from the node");
    assert!(tree[1].verify(&got));
    assert_eq!(report.tier, Some(FetchTier::Seed));
}

/// The verification shape from R1003 exactly: the client knows only the
/// node's bare `NodeId` (as a `permanent_seeds` entry would give it) and
/// resolves it by LAN mDNS; the CDN is unreachable. Ignored by default — it
/// needs working multicast on the host. Run with `--ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs LAN multicast (mDNS)"]
async fn a_client_fetches_a_pinned_blob_from_the_node_over_lan_discovery() {
    let src = tmpdir("src-lan");
    let tree = checkout(&src, 1, 8192);

    let node_ep = Endpoint::builder()
        .keypair(Keypair::generate())
        .discovery(Discovery::new().with_lan())
        .bind()
        .await
        .unwrap();
    let node_id_hex = hex::encode(node_ep.node_id().as_bytes());
    let node = BlobTransport::new(node_ep).await.unwrap();
    let node_class = disk_class(&tmpdir("cache-lan"), 1 << 20).await;
    node_class.pin_tree("pack", tree.clone()).await.unwrap();
    node_class.seed_tree_from_dirs("pack", &[src]).await.unwrap();
    node.serve_pinned_trees(&node_class).await.unwrap();

    let client_ep = Endpoint::builder()
        .keypair(Keypair::generate())
        .discovery(Discovery::new().with_lan())
        .bind()
        .await
        .unwrap();
    let client = BlobTransport::new(client_ep).await.unwrap();
    let client_class = AssetClass::register(AssetClassConfig {
        name: "r1003-lan-client",
        permanent_seeds: vec![node_id_hex],
        cdn_fallback: Some("http://127.0.0.1:9/{blake3}".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(client.attach_permanent_seeds(&client_class).attached, 1);

    // mDNS needs a moment to announce and be sighted; retry for up to 20s.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let (got, report) = client_class.asset(tree[0]).fetch_reported().await;
        if let Ok(bytes) = got {
            assert!(tree[0].verify(&bytes));
            assert_eq!(report.tier, Some(FetchTier::Seed));
            break;
        }
        assert!(std::time::Instant::now() < deadline, "never resolved the node over LAN");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
