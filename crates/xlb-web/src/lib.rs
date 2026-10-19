//! Browser storage tier for xlb.
//!
//! [`OpfsBlobStore`] persists blobs in the Origin Private File System, one
//! file per blob named by its hex BLAKE3, in a subdirectory per `AssetClass`.
//! Wrap it in [`xlb_core::RetainingCache`] (see [`open_cache`]) to get the
//! same pin / release-ring / LRU-budget retention as the native caches; that
//! layer also BLAKE3-verifies every read and deletes blobs that fail.
//!
//! The OPFS code is `cfg(target_arch = "wasm32")`; on native targets this
//! crate only re-exports `xlb-core`.
//!
//! @yah:ticket(R002-T6, "Run OpfsBlobStore in a real browser (wasm-bindgen-test headless)")
//! @yah:status(review)
//! @yah:at(2026-10-18T18:27:06Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R002)
//! @yah:next("Tier: Warrior")
//! @yah:next("R002-F4 OpfsBlobStore only typechecks for wasm32; add wasm-bindgen-test tests (put/get/delete/list, open_cache rebuild, eviction) and run them headless (wasm-pack test --headless --chrome or wasm-bindgen-test-runner + chromedriver). If no browser driver is installable here, record exactly what is missing.")
//! @yah:handoff("Added crates/xlb-web/tests/opfs.rs (8 wasm-bindgen-test cases, run_in_browser, main thread: put/get, delete present+missing, list sizes, class isolation+name validation, open_cache index rebuild (+shrunk-budget reopen evicts from OPFS), budget eviction deletes OPFS file, pinned survives eviction, corrupt file rejected+deleted) and wasm32-only dev-dep wasm-bindgen-test 0.3. Ran in headless Chrome 155: 8 passed, 0 failed. createWritable works on the main thread; no dedicated worker needed. No bugs found in xlb-web or RetainingCache.")
//! @yah:handoff("Environment: wasm-bindgen-test-runner 0.2.121 already installed; Chrome is at /Applications/Chrome.app (not 'Google Chrome.app'); chromedriver was missing, so fetched Chrome-for-Testing chromedriver 155.0.8059.84 to /tmp/r002t6-cd/chromedriver-mac-arm64/chromedriver. Added crates/xlb-web/webdriver.json pointing chromedriver at that Chrome binary (machine-specific path; chromedriver 404'd without it).")
//! @yah:handoff("Peer T5 briefly left xlb-core/src/store.rs policy() returning &RetentionPolicy mismatching retention.rs (E0308); it resolved itself, not touched by me.")
//! @yah:verify("CHROMEDRIVER=/tmp/r002t6-cd/chromedriver-mac-arm64/chromedriver CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner cargo test -p xlb-web --target wasm32-unknown-unknown: EXIT=0, 8 passed (/tmp/r002t6-5.log)")
//! @yah:verify("cargo test --no-run -p xlb-web --target wasm32-unknown-unknown: EXIT=0 (/tmp/r002t6-3.log)")
//! @yah:verify("native cargo test at oss/xlb root: EXIT=0, all green (/tmp/r002t6-6.log)")
//! @yah:verify("git add crates/xlb-web/Cargo.toml crates/xlb-web/tests crates/xlb-web/webdriver.json Cargo.lock && git commit -m 'xlb-web: headless-Chrome OPFS tests (R002-T6)'")
//! @yah:cleanup("crates/xlb-web/webdriver.json is tracked and hard-codes /Applications/Chrome.app/Contents/MacOS/Google Chrome (this machine's non-standard path); chromedriver lives in /tmp/r002t6-cd and won't survive reboot. Make it portable before committing: drop `binary` (standard install path is auto-found) or gitignore webdriver.json and add a small scripts/test-web.sh that locates Chrome + a matching chromedriver and sets CHROMEDRIVER / CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER.")

pub use xlb_core::{BlakeHash, BlobStore, RetainingCache, RetentionPolicy, StoreError};

#[cfg(target_arch = "wasm32")]
mod opfs;
#[cfg(target_arch = "wasm32")]
pub use opfs::{open_cache, OpfsBlobStore};
