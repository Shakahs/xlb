//! Origin tier against an in-process S3-style HTTP server that accepts only
//! correctly SigV4-signed requests and serves byte ranges and the outboard.

mod origin_server;

use std::{num::NonZeroUsize, path::Path, time::Duration};

use origin_server::{test_data, OriginServer, RangeBehavior};
use xlb::{AssetClass, AssetClassConfig, BlakeHash, Error, FetchTier, OriginConfig, RequestLimits};

const RANGE_BYTES: u64 = 256 * 1024;

async fn class(dir: &Path, origin: OriginConfig, budget: u64) -> AssetClass {
    let mut config = AssetClassConfig::new("models", dir);
    config.cache_budget_bytes = budget;
    config.gc_interval = Duration::from_millis(200);
    config.origin = Some(origin);
    AssetClass::register(config).await.expect("register class")
}

const BUDGET: u64 = 64 * 1024 * 1024;

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread")]
async fn one_blob_sustains_configured_origin_concurrency_without_overfilling_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let concurrency = 32;
    let data = test_data(48 * RANGE_BYTES as usize + 17);
    let hash = BlakeHash::hash(&data);
    let server = OriginServer::start(hash, data.clone()).await;
    let mut config = AssetClassConfig::new("models", dir.path());
    config.cache_budget_bytes = BUDGET;
    config.origin = Some(server.config(RANGE_BYTES, 1));
    config.request_limits = Some(RequestLimits::new(
        NonZeroUsize::new(40).unwrap(),
        NonZeroUsize::new(concurrency).unwrap(),
        NonZeroUsize::MIN,
    ).unwrap());
    let class = AssetClass::register(config).await.unwrap();
    server.pause_object_ranges(true);

    let asset = class.asset(hash);
    let fetch = tokio::spawn(async move { asset.fetch_to_file().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.object_ranges().len() <= concurrency && !fetch.is_finished() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("origin window opened");
    assert!(!fetch.is_finished(), "the fetch must survive a full origin window");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.object_ranges().len(), concurrency + 1, "one completed size probe and the active ranges");

    server.pause_object_ranges(false);
    let path = fetch.await.unwrap().expect("all ranges become available");
    assert_eq!(std::fs::read(path).unwrap(), data);
    assert_eq!(class.s3_request_stats().unwrap().peak_ranges, concurrency);
    class.shutdown().await;
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_s3_body_restarts_at_the_first_unverified_group() {
    let dir = tempfile::tempdir().unwrap();
    let data = test_data(2 * RANGE_BYTES as usize + 17);
    let hash = BlakeHash::hash(&data);
    let prefix = 64 * 1024;
    let behavior = RangeBehavior { disconnect_once: Some((0, prefix)), ..RangeBehavior::default() };
    let server = OriginServer::start_with(hash, data.clone(), behavior).await;
    let class = class(dir.path(), server.config(RANGE_BYTES, 4), BUDGET).await;
    let (result, report) = class.asset(hash).fetch_to_file_reported(None).await;
    assert_eq!(std::fs::read(result.expect("resume the interrupted range")).unwrap(), data);
    assert_eq!(report.origin_bytes, data.len() as u64);
    assert_eq!(server.object_ranges(), [
        (0, RANGE_BYTES),
        (prefix as u64, RANGE_BYTES),
        (RANGE_BYTES, 2 * RANGE_BYTES),
        (2 * RANGE_BYTES, data.len() as u64),
    ]);
    class.shutdown().await;
}

// [utest -> dsn~xlb-fork~1]
// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread")]
async fn a_blob_is_read_from_the_origin_as_signed_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let data = test_data(3 * 1024 * 1024 + 17);
    let hash = BlakeHash::hash(&data);
    let server = OriginServer::start(hash, data.clone()).await;
    let class = class(dir.path(), server.config(RANGE_BYTES, 4), BUDGET).await;

    // Two concurrent fetches of one hash share one transfer.
    let (asset_a, asset_b) = (class.asset(hash), class.asset(hash));
    let (first, second) = tokio::join!(
        asset_a.fetch_to_file_reported(None),
        asset_b.fetch_to_file_reported(None),
    );
    let path = first.0.expect("origin fetch");
    assert_eq!(second.0.expect("shared fetch"), path);
    assert_eq!(first.1.tier, Some(FetchTier::Origin));
    assert_eq!(
        (first.1.peer_bytes, first.1.origin_bytes),
        (0, data.len() as u64)
    );
    assert_eq!(path, class.cache_dir().join(hash.to_hex()));
    assert!(std::fs::read(&path).unwrap() == data);

    let seen = server.seen();
    let heads = seen.iter().filter(|s| s.method == "HEAD").count();
    assert_eq!(heads, 1, "one transfer, one size request: {seen:?}");
    let outboards = seen.iter().filter(|s| s.outboard).count();
    assert_eq!(outboards, 1, "the outboard is read once: {seen:?}");
    // The last chunk group is read first, to prove the size, and not again.
    let expected: Vec<(u64, u64)> = (0..data.len() as u64)
        .step_by(RANGE_BYTES as usize)
        .map(|start| (start, (start + RANGE_BYTES).min(data.len() as u64)))
        .collect();
    assert_eq!(
        seen.iter().find_map(|s| s.range),
        expected.last().copied(),
        "{seen:?}"
    );
    assert_eq!(
        server.object_ranges(),
        expected,
        "every range requested exactly once"
    );

    // The next fetch is a cache hit and does not touch the origin.
    let (again, report) = class.asset(hash).fetch_to_file_reported(None).await;
    assert_eq!(again.unwrap(), path);
    assert_eq!(report.tier, Some(FetchTier::Cache));
    assert_eq!(server.seen().len(), seen.len());
    class.shutdown().await;

    // A restart keeps the verified file and removes one whose content does
    // not match its name.
    let impostor = dir.path().join(BlakeHash::hash(b"impostor").to_hex());
    std::fs::write(&impostor, test_data(40 * 1024)).unwrap();
    let reopened = self::class(dir.path(), server.config(RANGE_BYTES, 4), BUDGET).await;
    assert!(reopened.asset(hash).is_cached());
    assert!(!impostor.exists());
    let (kept, report) = reopened.asset(hash).fetch_to_file_reported(None).await;
    assert_eq!(kept.unwrap(), path);
    assert_eq!(report.tier, Some(FetchTier::Cache));
    reopened.shutdown().await;

    let mut damaged = data.clone();
    damaged[17] ^= 1;
    std::fs::write(&path, damaged).unwrap();
    let reopened = self::class(dir.path(), server.config(RANGE_BYTES, 4), BUDGET).await;
    let (recovered, report) = reopened.asset(hash).fetch_to_file_reported(None).await;
    assert!(std::fs::read(recovered.unwrap()).unwrap() == data,
        "startup reused a same-length cache file whose bytes had changed");
    assert_eq!(report.tier, Some(FetchTier::Origin));
    reopened.shutdown().await;
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread")]
async fn origin_content_that_does_not_verify_fails_the_fetch_and_leaves_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let wanted = BlakeHash::hash(b"the content the caller asked for");
    // The object and its outboard agree with each other, not with the hash.
    let served = test_data(700 * 1024);
    let server = OriginServer::start(wanted, served).await;
    let class = class(dir.path(), server.config(RANGE_BYTES, 4), BUDGET).await;

    let result = class.asset(wanted).fetch_to_file().await;
    match result {
        Err(Error::OriginRejected { hash, .. }) => assert_eq!(hash, wanted),
        other => panic!("expected rejected origin content, got {other:?}"),
    }
    assert_eq!(
        server.object_ranges().len(),
        1,
        "the first range already fails verification"
    );
    assert!(!class.cache_dir().join(wanted.to_hex()).exists());
    assert!(!class.asset(wanted).is_cached());
    class.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_warm_file_does_not_bypass_the_indexed_length_check() {
    let dir = tempfile::tempdir().unwrap();
    let data = test_data(48 * 1024 + 7);
    let hash = BlakeHash::hash(&data);
    let server = OriginServer::start(hash, data.clone()).await;
    let node = class(dir.path(), server.config(RANGE_BYTES, 1), BUDGET).await;
    let size = data.len() as u64;
    let correct = node.indexed_chunk(hash, size);
    let path = correct.fetch_to_file().await.unwrap();
    assert!(node.indexed_chunk(hash, size - 1).fetch_to_file().await.is_err(),
        "a cache hit must reject metadata that names a different length");
    assert_eq!(correct.fetch_to_file().await.unwrap(), path);
    assert_eq!(std::fs::read(path).unwrap(), data);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blob_larger_than_the_budget_is_an_error_and_a_miss_names_the_origin() {
    let dir = tempfile::tempdir().unwrap();
    let data = test_data(300 * 1024);
    let hash = BlakeHash::hash(&data);
    let server = OriginServer::start(hash, data.clone()).await;
    let class = class(dir.path(), server.config(RANGE_BYTES, 4), 100 * 1024).await;

    let result = class.asset(hash).fetch_to_file().await;
    assert!(
        matches!(
            result,
            Err(Error::ExceedsBudget {
                size: 307_200,
                budget_bytes: 102_400,
                ..
            })
        ),
        "{result:?}"
    );
    assert!(
        server.seen().iter().all(|s| s.method == "HEAD"),
        "nothing is downloaded for a blob that cannot be cached"
    );

    let absent = BlakeHash::hash(b"not at the origin");
    let result = class.asset(absent).fetch_to_file().await;
    assert!(
        matches!(&result, Err(Error::FetchFailed { detail, .. }) if detail.contains("origin")),
        "{result:?}"
    );
    class.shutdown().await;
}
