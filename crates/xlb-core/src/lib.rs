//! Platform-neutral core of xlb: nothing here touches tokio, the filesystem,
//! iroh, reqwest or mshr, so it compiles for `wasm32-unknown-unknown`.
//!
//! The native `xlb` crate re-exports these types from its root; the browser
//! client (`xlb-web`) builds its OPFS cache on [`RetainingCache`].

pub mod hash;
pub mod refs;
pub mod retention;
pub mod store;

pub use hash::{BlakeHash, InvalidHash};
pub use refs::{Ref, Resolution, ResolveError};
pub use retention::{PinSetUsage, RetentionIndex, RetentionPolicy, RetentionUsage};
pub use store::{BlobStore, MaybeSend, MemBlobStore, RetainingCache, StoreError};
