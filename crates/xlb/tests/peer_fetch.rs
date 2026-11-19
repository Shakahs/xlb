//! Peer tier over loopback UDP: a node reads a multi-megabyte blob from two
//! peers into a plain file, and a node outside the peer set is refused.

use std::{
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use xlb::{
    AssetClass, AssetClassConfig, BlakeHash, Error, FetchProgress, FetchTier, PeerAddr,
    PeerConfig, ProgressSink, RelaySetting,
};

/// Deterministic bytes that are not a repeating block.
fn test_data(len: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

async fn node(name: &'static str, dir: &Path) -> AssetClass {
    let mut config = AssetClassConfig::new(name, dir.join(name));
    config.gc_interval = Duration::from_millis(200);
    config.peer = Some(PeerConfig {
        bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        relay: RelaySetting::Disabled,
        // Several segments, so the blob is read from both seeders at once.
        segment_bytes: 1024 * 1024,
        concurrency: 4,
        connect_timeout: Duration::from_secs(5),
        stall_timeout: Duration::from_secs(5),
    });
    AssetClass::register(config).await.expect("register class")
}

fn addr(class: &AssetClass) -> PeerAddr {
    PeerAddr {
        node_id: class.node_id().expect("class has a peer tier"),
        addrs: class.bound_addrs(),
        relay_url: None,    }
}

/// Give `class` the blob through a pinned tree seeded from `source`.
async fn seed(class: &AssetClass, hash: BlakeHash, source: &Path) {
    class.pin_tree("seed", [hash]).await.expect("pin tree");
    let report = class
        .seed_tree_from_dirs("seed", &[source.to_path_buf()])
        .await
        .expect("seed tree");
    assert_eq!(report.ingested, 1, "{report:?}");
}

// [utest -> dsn~xlb-peer-reach~1]
#[tokio::test(flavor = "multi_thread")]
async fn a_node_serves_the_nodes_it_admits_whether_or_not_it_reads_from_them() {
    let dir = tempfile::tempdir().unwrap();
    let data = test_data(2 * 1024 * 1024 + 7);
    let hash = BlakeHash::hash(&data);
    let source = dir.path().join("source");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(source.join(hash.to_hex()), &data).unwrap();

    let seeder = node("seeder", dir.path()).await;
    let admitted = node("admitted", dir.path()).await;
    let source_only = node("source-only", dir.path()).await;
    seed(&seeder, hash, &source).await;

    // The seeder reads from `source_only` and serves only `admitted`, whose
    // address it does not know.
    seeder.set_peer_sets(
        vec![addr(&source_only)],
        vec![admitted.node_id().expect("node id")],
    );
    admitted.set_peers(vec![addr(&seeder)]);
    source_only.set_peers(vec![addr(&seeder)]);

    let served = admitted.asset(hash).fetch_to_file().await.expect("admitted");
    assert!(std::fs::read(&served).unwrap() == data);

    let refused = source_only.asset(hash).fetch_to_file().await;
    assert!(
        matches!(refused, Err(Error::FetchFailed { .. })),
        "{refused:?}"
    );
    assert!(!source_only.cache_dir().join(hash.to_hex()).exists());

    for class in [seeder, admitted, source_only] {
        class.shutdown().await;
    }
}

// [utest -> dsn~xlb-fork~1]
#[tokio::test(flavor = "multi_thread")]
async fn a_blob_is_read_from_peers_into_a_plain_file_and_outsiders_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let data = test_data(5 * 1024 * 1024 + 123);
    let hash = BlakeHash::hash(&data);
    let source = dir.path().join("source");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(source.join(hash.to_hex()), &data).unwrap();

    let seeder_a = node("seeder-a", dir.path()).await;
    let seeder_b = node("seeder-b", dir.path()).await;
    let fetcher = node("fetcher", dir.path()).await;
    let outsider = node("outsider", dir.path()).await;
    seed(&seeder_a, hash, &source).await;
    seed(&seeder_b, hash, &source).await;

    // The seeders admit only the fetcher. The outsider knows where the
    // seeders are but is in neither peer set.
    seeder_a.set_peers(vec![addr(&fetcher)]);
    seeder_b.set_peers(vec![addr(&fetcher)]);
    fetcher.set_peers(vec![addr(&seeder_a), addr(&seeder_b)]);
    outsider.set_peers(vec![addr(&seeder_a), addr(&seeder_b)]);

    let events: Arc<Mutex<Vec<FetchProgress>>> = Arc::default();
    let captured = events.clone();
    let sink: ProgressSink = Arc::new(move |p| captured.lock().unwrap().push(p));
    let (result, report) = fetcher
        .asset(hash)
        .fetch_to_file_reported(Some(sink))
        .await;
    let path = result.expect("peer fetch");

    assert_eq!(report.tier, Some(FetchTier::Peer));
    assert_eq!(report.bytes_served, data.len() as u64);
    assert_eq!(path, fetcher.cache_dir().join(hash.to_hex()));
    assert!(
        std::fs::read(&path).unwrap() == data,
        "the plain file holds the blob"
    );
    let events = events.lock().unwrap().clone();
    assert!(events.iter().all(|p| p.tier == FetchTier::Peer));
    assert!(events
        .iter()
        .all(|p| p.total == Some(data.len() as u64) && p.bytes_so_far <= data.len() as u64));

    // A second fetch is a cache hit on the same file.
    let (again, report) = fetcher.asset(hash).fetch_to_file_reported(None).await;
    assert_eq!(again.unwrap(), path);
    assert_eq!(report.tier, Some(FetchTier::Cache));

    // The outsider has no origin, so a refused peer tier is a failed fetch.
    let refused = outsider.asset(hash).fetch_to_file().await;
    assert!(
        matches!(refused, Err(Error::FetchFailed { .. })),
        "{refused:?}"
    );
    assert!(!outsider.cache_dir().join(hash.to_hex()).exists());

    // Once admitted, the same node is served.
    seeder_a.set_peers(vec![addr(&fetcher), addr(&outsider)]);
    seeder_b.set_peers(vec![addr(&fetcher), addr(&outsider)]);
    let admitted = outsider.asset(hash).fetch_to_file().await.expect("admitted");
    assert!(std::fs::read(&admitted).unwrap() == data);

    // The fetcher serves what it fetched: a complete blob needs no pin.
    let late = node("late", dir.path()).await;
    fetcher.set_peers(vec![addr(&late)]);
    late.set_peers(vec![addr(&fetcher)]);
    let relayed = late.asset(hash).fetch_to_file().await.expect("relayed");
    assert!(std::fs::read(&relayed).unwrap() == data);

    for class in [seeder_a, seeder_b, fetcher, outsider, late] {
        class.shutdown().await;
    }
}
