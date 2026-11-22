# XLB

XLB serves indexed model files and OCI images from a node-local content-addressed cache. S3-compatible storage holds the authoritative indexes and chunks. A node can serve as soon as its local services are ready, including when it has no peers.

Each index gives the BLAKE3 digest and length of every chunk, up to 8 MiB per chunk. XLB checks both before caching a chunk in Foyer or advertising it to peers. Concurrent requests for the same digest share one transfer. A node can serve any chunk it holds while the rest of the file is still downloading.

P2panda synchronizes inventory for same-location and global peers and supplies the endpoint for direct QUIC transfers. Local peers serve admitted nodes in their own locality. Global peers can serve admitted nodes in other localities. The transfer state machine selects one source at a time, tries eligible same-location peers first, and uses OpenDAL to fetch from S3 when peer sourcing is disabled or no attempt succeeds. The peer attempt timeout is five seconds, a failed peer is excluded for 60 seconds, and a chunk gets at most five peer attempts.

The daemon connects the cache to the model mount, containerd snapshotter, and OCI mirror. Ractor supervises the services and orders shutdown so cache consumers release their resources before Foyer closes. The host service manager restarts the daemon after an unrecoverable service failure.

Run the library tests from the enclosing Bazel workspace with `bb test //vendor/xlb/crates/xlb:xlb_tests`.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../../LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this crate by you, as defined in the Apache-2.0 license, shall be dual-licensed as above,
without any additional terms or conditions.
