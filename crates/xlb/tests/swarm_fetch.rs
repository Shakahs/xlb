//! Swarm fetch over loopback: nodes that fetch one blob serve each other the
//! parts they hold and divide the origin reads. Every node has its own
//! in-process origin serving the same object, so the origin requests of one
//! node can be counted and stalled apart from the other's.

mod origin_server;

use std::{
    net::SocketAddr,
    path::Path,
    time::{Duration, Instant},
};

use origin_server::{test_data, OriginServer, RangeBehavior};
use xlb::{
    AssetClass, AssetClassConfig, BlakeHash, FetchTier, PeerAddr, PeerConfig, RelaySetting,
};

const SEGMENT_BYTES: u64 = 256 * 1024;
/// Two origin ranges per segment.
const RANGE_BYTES: u64 = 128 * 1024;
const SEGMENTS: u64 = 24;
/// The blob: whole segments and a short last chunk group.
const BLOB_BYTES: u64 = SEGMENTS * SEGMENT_BYTES + 1000;
/// Segments a stalled origin still serves.
const SERVED_SEGMENTS: u64 = 8;

async fn node(name: &'static str, dir: &Path, origin: &OriginServer, stall: Duration) -> AssetClass {
    let mut config = AssetClassConfig::new(name, dir.join(name));
    config.gc_interval = Duration::from_millis(200);
    config.peer = Some(PeerConfig {
        bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        relay: RelaySetting::Disabled,
        segment_bytes: SEGMENT_BYTES,
        concurrency: 4,
        connect_timeout: Duration::from_secs(5),
        stall_timeout: stall,
    });
    // Four ranges in flight: two segments at a time.
    config.origin = Some(origin.config(RANGE_BYTES, 4));
    AssetClass::register(config).await.expect("register class")
}

fn addr(class: &AssetClass) -> PeerAddr {
    PeerAddr {
        node_id: class.node_id().expect("class has a peer tier"),
        addrs: class.bound_addrs(),
        relay_url: None,    }
}

fn link(a: &AssetClass, b: &AssetClass) {
    a.set_peers(vec![addr(b)]);
    b.set_peers(vec![addr(a)]);
}

/// The origin ranges of the whole segments from segment `first` on.
fn origin_ranges(first: u64) -> Vec<(u64, u64)> {
    (first * SEGMENT_BYTES..SEGMENTS * SEGMENT_BYTES)
        .step_by(RANGE_BYTES as usize)
        .map(|start| (start, start + RANGE_BYTES))
        .collect()
}

/// An origin that answers only the first [`SERVED_SEGMENTS`] segments and
/// the last chunk group, and a node whose fetch from it has stored those
/// segments and is stuck on the next ones. The fetch never finishes.
async fn stalled_fetcher(
    dir: &Path,
    hash: BlakeHash,
    data: &[u8],
) -> (OriginServer, AssetClass, tokio::task::JoinHandle<()>) {
    let behavior = RangeBehavior {
        hang_from: Some(SERVED_SEGMENTS * SEGMENT_BYTES),
        ..RangeBehavior::default()
    };
    let origin = OriginServer::start_with(hash, data.to_vec(), behavior).await;
    let holder = node("holder", dir, &origin, Duration::from_secs(600)).await;
    let asset = holder.asset(hash);
    let fetch = tokio::spawn(async move {
        let _ = asset.fetch_to_file().await;
    });
    // Segments are read in order, two at a time. Once all four ranges of the
    // first two unanswered segments are requested, every answered segment
    // has been stored.
    let stuck = origin_ranges(SERVED_SEGMENTS)[..4].to_vec();
    tokio::time::timeout(Duration::from_secs(60), async {
        while !stuck.iter().all(|range| origin.object_ranges().contains(range)) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the holder reads its answered segments");
    (origin, holder, fetch)
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_blob_is_served_to_a_peer_that_reads_the_rest_from_the_origin() {
    let dir = tempfile::tempdir().unwrap();
    let data = test_data(BLOB_BYTES as usize);
    let hash = BlakeHash::hash(&data);
    let (_holder_origin, holder, stuck_fetch) = stalled_fetcher(dir.path(), hash, &data).await;
    assert!(!holder.asset(hash).is_cached());

    let stall = Duration::from_secs(1);
    let origin = OriginServer::start(hash, data.clone()).await;
    let fetcher = node("fetcher", dir.path(), &origin, stall).await;
    link(&holder, &fetcher);

    let started = Instant::now();
    let (result, report) = fetcher.asset(hash).fetch_to_file_reported(None).await;
    let path = result.expect("fetch from a partial peer and the origin");
    assert!(std::fs::read(&path).unwrap() == data);

    // The holder's segments and the size came from the holder; nothing it
    // held was asked of the origin.
    assert!(
        report.peer_bytes >= SERVED_SEGMENTS * SEGMENT_BYTES,
        "{report:?}"
    );
    assert!(
        origin.seen().iter().all(|seen| seen.method != "HEAD"),
        "the size is proven by the peer"
    );
    // Every other segment came from the origin exactly once: this node's
    // own at once, and those assigned to the stuck holder after the stall
    // timeout.
    assert_eq!(origin.object_ranges(), origin_ranges(SERVED_SEGMENTS));
    assert_eq!(
        report.origin_bytes,
        (SEGMENTS - SERVED_SEGMENTS) * SEGMENT_BYTES,
        "{report:?}"
    );
    assert_eq!(report.tier, Some(FetchTier::Origin));
    assert!(
        started.elapsed() >= stall,
        "segments assigned to the holder are left to it for the stall timeout"
    );
    assert!(!holder.asset(hash).is_cached(), "the holder is still partial");

    stuck_fetch.abort();
    fetcher.shutdown().await;
    holder.shutdown().await;
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread")]
async fn two_concurrent_fetchers_read_each_origin_segment_about_once() {
    let dir = tempfile::tempdir().unwrap();
    let data = test_data(BLOB_BYTES as usize);
    let hash = BlakeHash::hash(&data);
    // A range takes long enough for a stored segment to be seen by the other
    // node before that node would read it.
    let behavior = RangeBehavior {
        delay: Duration::from_millis(20),
        ..RangeBehavior::default()
    };
    let origin_a = OriginServer::start_with(hash, data.clone(), behavior).await;
    let origin_b = OriginServer::start_with(hash, data.clone(), behavior).await;
    // No takeover within this test: a segment is read by its assigned reader.
    let stall = Duration::from_secs(600);
    let node_a = node("node-a", dir.path(), &origin_a, stall).await;
    let node_b = node("node-b", dir.path(), &origin_b, stall).await;
    link(&node_a, &node_b);

    let (asset_a, asset_b) = (node_a.asset(hash), node_b.asset(hash));
    let ((result_a, report_a), (result_b, report_b)) = tokio::join!(
        asset_a.fetch_to_file_reported(None),
        asset_b.fetch_to_file_reported(None),
    );
    assert!(std::fs::read(result_a.expect("fetch on node a")).unwrap() == data);
    assert!(std::fs::read(result_b.expect("fetch on node b")).unwrap() == data);

    // One fetcher alone would send this many ranged requests.
    let alone = origin_ranges(0).len() + 1;
    let together = origin_a.object_ranges().len() + origin_b.object_ranges().len();
    assert!(
        together >= alone - 1 && together * 2 <= alone * 3,
        "{together} origin requests by two fetchers, {alone} by one"
    );
    assert!(
        report_a.origin_bytes + report_b.origin_bytes <= BLOB_BYTES * 3 / 2,
        "{report_a:?} {report_b:?}"
    );
    // Each node got the segments it did not read itself from the other.
    for report in [&report_a, &report_b] {
        assert!(report.peer_bytes > 0, "{report:?}");
        assert!(
            report.peer_bytes + report.origin_bytes >= SEGMENTS * SEGMENT_BYTES,
            "{report:?}"
        );
    }

    node_a.shutdown().await;
    node_b.shutdown().await;
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread")]
async fn a_fetcher_reads_the_segments_of_a_reader_that_left() {
    let dir = tempfile::tempdir().unwrap();
    let data = test_data(BLOB_BYTES as usize);
    let hash = BlakeHash::hash(&data);
    let (_holder_origin, holder, stuck_fetch) = stalled_fetcher(dir.path(), hash, &data).await;

    // The stall timeout is far beyond this test, so only the holder's
    // departure can free the segments assigned to it.
    let origin = OriginServer::start(hash, data.clone()).await;
    let fetcher = node("fetcher", dir.path(), &origin, Duration::from_secs(600)).await;
    link(&holder, &fetcher);
    let asset = fetcher.asset(hash);
    let fetch = tokio::spawn(async move { asset.fetch_to_file_reported(None).await });

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !fetch.is_finished(),
        "segments assigned to the stuck holder are left to it"
    );

    stuck_fetch.abort();
    holder.shutdown().await;
    let (result, report) = tokio::time::timeout(Duration::from_secs(60), fetch)
        .await
        .expect("the fetch completes once the holder has left")
        .expect("fetch task");
    assert!(std::fs::read(result.expect("fetch after the holder left")).unwrap() == data);
    assert!(report.peer_bytes > 0 && report.origin_bytes > 0, "{report:?}");
    fetcher.shutdown().await;
}
