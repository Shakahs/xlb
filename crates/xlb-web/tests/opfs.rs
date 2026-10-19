//! Browser tests for [`OpfsBlobStore`]. Run with:
//! `CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//!  cargo test -p xlb-web --target wasm32-unknown-unknown`
#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;
use xlb_web::{open_cache, BlakeHash, BlobStore, OpfsBlobStore, RetentionPolicy, StoreError};

wasm_bindgen_test_configure!(run_in_browser);

fn blob(seed: u8, len: usize) -> Vec<u8> {
    vec![seed; len]
}

/// A fresh, empty class directory per test (OPFS persists across tests in
/// one browser profile, so each test owns a unique name and wipes it first).
async fn fresh_store(class: &str) -> OpfsBlobStore {
    let s = OpfsBlobStore::open(class).await.unwrap();
    for (h, _) in s.list().await.unwrap() {
        s.delete(&h).await.unwrap();
    }
    s
}

#[wasm_bindgen_test]
async fn put_get_roundtrip() {
    let s = fresh_store("t-roundtrip").await;
    let data = b"hello opfs".to_vec();
    let h = BlakeHash::hash(&data);
    assert_eq!(s.get(&h).await.unwrap(), None);
    s.put(&h, &data).await.unwrap();
    assert_eq!(s.get(&h).await.unwrap().unwrap(), data);
    // overwrite is idempotent
    s.put(&h, &data).await.unwrap();
    assert_eq!(s.get(&h).await.unwrap().unwrap(), data);
}

#[wasm_bindgen_test]
async fn delete_present_and_missing() {
    let s = fresh_store("t-delete").await;
    let data = blob(1, 8);
    let h = BlakeHash::hash(&data);
    s.put(&h, &data).await.unwrap();
    s.delete(&h).await.unwrap();
    assert_eq!(s.get(&h).await.unwrap(), None);
    // deleting a missing blob is not an error
    s.delete(&h).await.unwrap();
    s.delete(&BlakeHash::hash(b"never stored")).await.unwrap();
}

#[wasm_bindgen_test]
async fn list_returns_sizes() {
    let s = fresh_store("t-list").await;
    assert!(s.list().await.unwrap().is_empty());
    let mut want = Vec::new();
    for (seed, len) in [(1u8, 5usize), (2, 300), (3, 0)] {
        let d = blob(seed, len);
        let h = BlakeHash::hash(&d);
        s.put(&h, &d).await.unwrap();
        want.push((h.to_hex(), len as u64));
    }
    let mut got: Vec<_> = s
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|(h, n)| (h.to_hex(), n))
        .collect();
    got.sort();
    want.sort();
    assert_eq!(got, want);
}

#[wasm_bindgen_test]
async fn class_dirs_are_isolated_and_names_validated() {
    let a = fresh_store("t-iso-a").await;
    let b = fresh_store("t-iso-b").await;
    let d = blob(7, 4);
    let h = BlakeHash::hash(&d);
    a.put(&h, &d).await.unwrap();
    assert_eq!(b.get(&h).await.unwrap(), None);
    for bad in ["", "..", ".", "a/b"] {
        assert!(OpfsBlobStore::open(bad).await.is_err(), "{bad:?}");
    }
}

#[wasm_bindgen_test]
async fn open_cache_rebuilds_index_from_existing_files() {
    let class = "t-rebuild";
    let s = fresh_store(class).await;
    let (d1, d2) = (blob(1, 10), blob(2, 20));
    let (h1, h2) = (BlakeHash::hash(&d1), BlakeHash::hash(&d2));
    s.put(&h1, &d1).await.unwrap();
    s.put(&h2, &d2).await.unwrap();

    let c = open_cache(class, RetentionPolicy::new(1000, 1)).await.unwrap();
    assert!(c.contains(&h1) && c.contains(&h2));
    assert_eq!(c.usage().entries, 2);
    assert_eq!(c.usage().bytes, 30);
    assert_eq!(c.get(&h2).await.unwrap().unwrap(), d2);

    // reopening with a smaller budget evicts immediately, and from OPFS
    let c = open_cache(class, RetentionPolicy::new(20, 1)).await.unwrap();
    assert_eq!(c.usage().entries, 1);
    let left = c.store().list().await.unwrap();
    assert_eq!(left.len(), 1);
}

#[wasm_bindgen_test]
async fn budget_eviction_deletes_opfs_file() {
    let class = "t-evict";
    fresh_store(class).await;
    let c = open_cache(class, RetentionPolicy::new(20, 1)).await.unwrap();
    let a = c.put(&blob(1, 10)).await.unwrap();
    let b = c.put(&blob(2, 10)).await.unwrap();
    c.get(&a).await.unwrap(); // a is MRU
    let d = c.put(&blob(3, 10)).await.unwrap();
    assert!(c.contains(&a) && c.contains(&d) && !c.contains(&b));
    assert_eq!(c.store().get(&b).await.unwrap(), None, "file gone from OPFS");
    assert_eq!(c.store().list().await.unwrap().len(), 2);
    assert!(matches!(
        c.put(&blob(4, 21)).await,
        Err(StoreError::TooLarge { .. })
    ));
}

#[wasm_bindgen_test]
async fn pinned_blob_survives_eviction() {
    let class = "t-pinned";
    fresh_store(class).await;
    let c = open_cache(class, RetentionPolicy::new(10, 1)).await.unwrap();
    let p = BlakeHash::hash(&blob(9, 50));
    c.pin(p);
    c.put(&blob(9, 50)).await.unwrap();
    c.put(&blob(1, 10)).await.unwrap();
    c.put(&blob(2, 10)).await.unwrap();
    assert!(c.is_pinned(&p) && c.contains(&p));
    assert_eq!(c.store().get(&p).await.unwrap().unwrap(), blob(9, 50));
    c.unpin(&p).await.unwrap();
    assert_eq!(c.store().get(&p).await.unwrap(), None);
}

#[wasm_bindgen_test]
async fn corrupt_file_is_rejected_and_dropped() {
    let class = "t-corrupt";
    let s = fresh_store(class).await;
    let good = blob(5, 16);
    let h = BlakeHash::hash(&good);
    // store garbage under the good blob's name
    s.put(&h, b"not the right bytes").await.unwrap();
    let c = open_cache(class, RetentionPolicy::new(1000, 1)).await.unwrap();
    assert!(c.contains(&h));
    assert_eq!(c.get(&h).await.unwrap(), None);
    assert!(!c.contains(&h));
    assert_eq!(c.store().get(&h).await.unwrap(), None, "corrupt file deleted");
}
