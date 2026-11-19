//! Origin tier against an in-process S3-style HTTP server that accepts only
//! correctly SigV4-signed requests and serves byte ranges and the outboard.

mod origin_server;

use std::{path::Path, time::Duration};

use origin_server::{test_data, OriginServer};
use xlb::{AssetClass, AssetClassConfig, BlakeHash, Error, FetchTier, OriginConfig};

const RANGE_BYTES: u64 = 256 * 1024;

async fn class(dir: &Path, origin: OriginConfig, budget: u64) -> AssetClass {
    let mut config = AssetClassConfig::new("models", dir);
    config.cache_budget_bytes = budget;
    config.gc_interval = Duration::from_millis(200);
    config.origin = Some(origin);
    AssetClass::register(config).await.expect("register class")
}

const BUDGET: u64 = 64 * 1024 * 1024;

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
