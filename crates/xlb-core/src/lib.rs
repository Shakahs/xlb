//! Platform-neutral core of xlb: nothing here touches tokio, the filesystem,
//! iroh or reqwest.
//!
//! The native `xlb` crate re-exports these types from its root.

pub mod hash;
pub mod retention;

pub use hash::{BlakeHash, InvalidHash};
pub use retention::{PinSetUsage, RetentionIndex, RetentionPolicy, RetentionUsage};
