//! Whole CAS chunk transfers with real p2panda nodes and a signed S3 origin.

#[path = "../tests/origin_server/mod.rs"]
mod origin_server;

use std::{net::SocketAddr, path::Path, time::Duration};

use tokio::time::Instant;
use xlb_core::{PeerReach, image::IMAGE_CHUNK_BYTES};

use super::*;
use origin_server::{FirstGet, OriginServer, test_data};

const BIND: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 0));

async fn wait_for_records(receiver: &AssetClass, hash: BlakeHash, expected: &[NodeId]) {
    let peers = receiver.0.peers.as_ref().unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let candidates = peers.candidates(hash);
            if expected.iter().all(|node| candidates.contains(node)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("receive inventory from the expected holders");
}

async fn peer_node(path: &Path, location: &str, reach: PeerReach, origin: Option<OriginConfig>) -> AssetClass {
    peer_node_with_memory(path, location, reach, origin, 512).await
}

async fn peer_node_with_memory(path: &Path, location: &str, reach: PeerReach, origin: Option<OriginConfig>, memory_mib: usize) -> AssetClass {
    let mut config = AssetClassConfig::new("chunks", path);
    config.cache_budget_bytes = 128 << 20;
    config.cas_memory_bytes = memory_mib << 20;
    config.origin = origin;
    config.peer = Some(PeerConfig { bind_addr: BIND, relay: RelaySetting::Disabled, ..PeerConfig::default() });
    let node = AssetClass::register(config).await.unwrap();
    node.set_node_topology(Some(NodeTopology {
        provider: location.split('/').next().unwrap().into(),
        location: location.into(), region: None, zone: None, reach,
    }));
    node
}

async fn cache_data(holder: &AssetClass, hash: BlakeHash, body: Bytes) {
    holder.0.cache.insert(hash, body.len() as u64, body).await.unwrap();
    holder.0.peers.as_ref().unwrap().announce(hash);
}

// [itest -> dsn~xlb-chunk-discovery~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejoining_inventory_keeps_the_live_holder_after_another_evicts() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let location = "aws/us-west-2a";
        let restart_path = directory.path().join("reader");
        let retiring = peer_node(&directory.path().join("retiring"), location, PeerReach::Local, None).await;
        let retaining = peer_node(&directory.path().join("retaining"), location, PeerReach::Local, None).await;
        let reader = peer_node(&restart_path, location, PeerReach::Local, None).await;
        let retiring_addr = cache_address(&retiring).await;
        let retaining_addr = cache_address(&retaining).await;
        let reader_addr = cache_address(&reader).await;
        let body = Bytes::from_static(b"chunk initially present on both holders");
        let digest = BlakeHash::hash(&body);
        let anchor = Bytes::from_static(b"the retiring holder published after its eviction");
        let anchor_hash = BlakeHash::hash(&anchor);
        cache_data(&retiring, digest, body.clone()).await;
        cache_data(&retaining, digest, body.clone()).await;

        retiring.set_peers(vec![retaining_addr.clone(), reader_addr.clone()]);
        retaining.set_peers(vec![retiring_addr.clone(), reader_addr]);
        reader.set_peers(vec![retiring_addr.clone(), retaining_addr.clone()]);
        wait_for_records(&reader, digest, &[retiring_addr.node_id, retaining_addr.node_id]).await;
        wait_for_records(&retaining, digest, &[retiring_addr.node_id]).await;
        reader.shutdown().await.unwrap();
        drop(reader);

        retiring.0.cache.evict_for_test(digest);
        assert!(!retiring.0.cache.may_hold(digest));
        let live_inventory = retaining.0.peers.as_ref().unwrap();
        while live_inventory.knows_holder(digest, location, retiring_addr.node_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cache_data(&retiring, anchor_hash, anchor).await;
        let reader = peer_node(&restart_path, location, PeerReach::Local, None).await;
        let reader_addr = cache_address(&reader).await;
        retiring.set_peers(vec![retaining_addr.clone(), reader_addr.clone()]);
        retaining.set_peers(vec![retiring_addr.clone(), reader_addr]);
        reader.set_peers(vec![retiring_addr.clone(), retaining_addr.clone()]);
        wait_for_records(&reader, anchor_hash, &[retiring_addr.node_id]).await;
        wait_for_records(&reader, digest, &[retaining_addr.node_id]).await;
        let candidates = reader.0.peers.as_ref().unwrap().candidates(digest);
        assert_eq!(candidates, [retaining_addr.node_id]);
        let (read, report) = reader.indexed_chunk(digest, body.len() as u64).read_chunk_reported().await;
        assert_eq!(read.unwrap(), body);
        assert_eq!(report.peers.get(&retaining_addr.node_id.to_string()), Some(&(body.len() as u64)));
        tokio::try_join!(reader.shutdown(), retiring.shutdown(), retaining.shutdown()).unwrap();
    }).await.expect("a returning node must learn the current holdings");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabling_peer_sourcing_uses_s3_even_with_a_known_holder() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let body = Bytes::from_static(b"one chunk with two available sources");
        let hash = BlakeHash::hash(&body);
        let origin = OriginServer::start(hash, body.to_vec()).await;
        let locality = "aws/us-west-2a";
        let holder = peer_node(&directory.path().join("holder"), locality, PeerReach::Local, None).await;
        cache_data(&holder, hash, body.clone()).await;

        let mut config = AssetClassConfig::new("chunks", directory.path().join("reader"));
        config.origin = Some(origin.config(1));
        config.peer = Some(PeerConfig {
            bind_addr: BIND, relay: RelaySetting::Disabled, disable_sourcing: true,
            ..PeerConfig::default()
        });
        let reader = AssetClass::register(config).await.unwrap();
        reader.set_node_topology(Some(NodeTopology {
            provider: "aws".into(), location: locality.into(), region: None, zone: None, reach: PeerReach::Local,
        }));
        let address = cache_address(&holder).await;
        holder.set_peers(vec![cache_address(&reader).await]);
        reader.set_peers(vec![address.clone()]);
        let peers = reader.0.peers.as_ref().unwrap();
        while !peers.knows_holder(hash, locality, address.node_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let (result, report) = reader.indexed_chunk(hash, body.len() as u64).read_chunk_reported().await;
        assert_eq!(result.unwrap(), body);
        assert_eq!((report.origin_bytes, report.peer_bytes), (body.len() as u64, 0));
        assert_eq!(origin.gets(), 1);
        tokio::try_join!(holder.shutdown(), reader.shutdown()).unwrap();
    }).await.expect("an S3-only reader must still serve an indexed chunk");
}

// [itest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_serves_a_completed_chunk_while_the_next_chunk_is_downloading() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let complete = Bytes::from(test_data(IMAGE_CHUNK_BYTES));
        let tail = test_data(96 * 1024);
        let first = BlakeHash::hash(&complete);
        let second = BlakeHash::hash(&tail);
        let origin = OriginServer::start(second, tail.clone()).await;
        let locality = "aws/us-west-2a";
        let holder = peer_node(&directory.path().join("holder"), locality, PeerReach::Local, Some(origin.config(1))).await;
        let reader = peer_node(&directory.path().join("reader"), locality, PeerReach::Local, None).await;
        holder.set_peers(vec![cache_address(&reader).await]);
        reader.set_peers(vec![cache_address(&holder).await]);
        cache_data(&holder, first, complete.clone()).await;

        origin.pause(true);
        let pending = holder.indexed_chunk(second, tail.len() as u64);
        let download = tokio::spawn(async move { pending.read_chunk().await });
        while origin.gets() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let holder_id = holder.node_id().unwrap();
        wait_for_records(&reader, first, &[holder_id]).await;
        let (result, report) = reader.indexed_chunk(first, complete.len() as u64).read_chunk_reported().await;
        assert_eq!(result.unwrap(), complete);
        assert_eq!(report.peer_bytes, complete.len() as u64);
        assert!(!download.is_finished());
        assert!(!holder.indexed_chunk(second, tail.len() as u64).is_cached());
        assert!(!reader.0.peers.as_ref().unwrap().knows_holder(second, locality, holder_id));

        origin.pause(false);
        assert_eq!(download.await.unwrap().unwrap().as_ref(), tail);
        wait_for_records(&reader, second, &[holder_id]).await;
        tokio::try_join!(holder.shutdown(), reader.shutdown()).unwrap();
    }).await.expect("holding one complete CAS chunk is sufficient to serve it");
}

// [itest -> dsn~xlb-supervised-lifecycle~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_transport_is_replaced_without_restarting_the_daemon() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let locality = "aws/us-west-2a";
        let node = peer_node(directory.path(), locality, PeerReach::Local, None).await;
        let initial = cache_address(&node).await;
        let peers = node.0.peers.as_ref().unwrap();
        peers.transport_endpoint().unwrap().close().await;
        loop {
            if let Some(endpoint) = peers.transport_endpoint()
                .filter(|endpoint| !endpoint.is_closed() && !endpoint.bound_sockets().is_empty())
            {
                assert_eq!(endpoint.id(), initial.node_id);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        node.shutdown().await.unwrap();
    }).await.expect("an endpoint loss must recover with the same node identity");
}

#[derive(Debug)]
struct StalledProvider(std::sync::Arc<tokio::sync::Notify>);

impl iroh::protocol::ProtocolHandler for StalledProvider {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> std::result::Result<(), iroh::protocol::AcceptError> {
        let (_send, mut receive) = connection.accept_bi().await?;
        assert_eq!(receive.read_to_end(32).await.unwrap().len(), 32);
        self.0.notify_one();
        connection.closed().await;
        Ok(())
    }
}

async fn fixture_endpoint(book: p2panda_net::AddressBook) -> p2panda_net::Endpoint {
    p2panda_net::Endpoint::builder(book)
        .network_id(crate::panda_network::inventory_network_id("chunks"))
        .config(p2panda_net::iroh_endpoint::IrohConfig {
            bind_ip_v4: std::net::Ipv4Addr::LOCALHOST,
            bind_port_v4: 0,
            ..Default::default()
        }).spawn().await.unwrap()
}

// [itest -> dsn~xlb-chunk-discovery~1,dsn~xlb-supervised-lifecycle~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_peer_times_out_before_s3_fetches_the_chunk() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let directory = tempfile::tempdir().unwrap();
        let body = test_data(96 * 1024);
        let digest = BlakeHash::hash(&body);
        let origin = OriginServer::start(digest, body.clone()).await;
        let locality = "aws/us-west-2a";
        let reader = peer_node(directory.path(), locality, PeerReach::Local, Some(origin.config(2))).await;
        let book = p2panda_net::AddressBook::builder().spawn().await.unwrap();
        let holder = fixture_endpoint(book).await;
        let requested = Arc::new(tokio::sync::Notify::new());
        holder.accept(crate::panda_chunks::CHUNK_PROTOCOL, StalledProvider(requested.clone())).await.unwrap();
        let endpoint = holder.endpoint().await.unwrap();
        let address = endpoint.addr();
        reader.set_peers(vec![PeerAddr {
            node_id: address.id,
            addrs: endpoint.bound_sockets(),
            relay_url: None,
        }]);
        let peers = reader.0.peers.as_ref().unwrap();
        peers.project_test_inventory(crate::panda_inventory::InventoryRecord {
            address: address.clone(),
            topology: NodeTopology {
                provider: "aws".into(), location: locality.into(), region: None, zone: None, reach: PeerReach::Local,
            },
            checkpoint: true,
            issued_at: p2panda_core::Timestamp::now(),
            added: vec![digest],
            removed: Vec::new(),
        });
        wait_for_records(&reader, digest, &[address.id]).await;
        let started = Instant::now();
        let asset = reader.indexed_chunk(digest, body.len() as u64);
        let downloading = tokio::spawn(async move { asset.read_chunk_reported().await });
        requested.notified().await;
        assert_eq!(origin.gets(), 0, "S3 must not race the peer");
        let (result, report) = downloading.await.unwrap();
        assert!(started.elapsed() >= crate::panda_chunks::ATTEMPT_TIMEOUT);
        assert_eq!(result.unwrap().as_ref(), body);
        assert_eq!((report.peer_bytes, report.origin_bytes), (0, body.len() as u64));
        assert_eq!(origin.gets(), 1);
        assert!(peers.candidates(digest).is_empty(), "the timed-out holder must be excluded");
        reader.shutdown().await.unwrap();
        endpoint.close().await;
    }).await.expect("the failed peer must give way to S3");
}

// [itest -> dsn~xlb-peer-reach~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_holder_refuses_a_direct_request_from_another_zone() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let holder = peer_node(&directory.path().join("holder"), "aws/us-west-2a", PeerReach::Local, None).await;
        let neighbor = peer_node(&directory.path().join("neighbor"), "aws/us-west-2a", PeerReach::Local, None).await;
        let body = Bytes::from_static(b"one verified CAS chunk");
        let digest = BlakeHash::hash(&body);
        cache_data(&holder, digest, body.clone()).await;
        holder.set_peers(vec![cache_address(&neighbor).await]);
        let address = cache_address(&holder).await;
        neighbor.set_peers(vec![address.clone()]);
        wait_for_records(&neighbor, digest, &[address.node_id]).await;
        let read = neighbor.indexed_chunk(digest, body.len() as u64).read_chunk().await.unwrap();
        assert_eq!(read, body);

        let book = p2panda_net::AddressBook::builder().spawn().await.unwrap();
        let address = iroh::EndpointAddr::from_parts(address.node_id, address.addrs.into_iter().map(iroh::TransportAddr::Ip));
        let node = p2panda_core::VerifyingKey::from_bytes(address.id.as_bytes()).unwrap();
        book.insert_node_info(p2panda_net::addrs::NodeInfo::from(address).bootstrap()).await.unwrap();
        let outside = fixture_endpoint(book).await;
        let protocol = crate::panda_chunks::CHUNK_PROTOCOL;
        let connection = outside.connect(node, protocol).await.unwrap();
        let refusal = connection.closed().await;
        assert!(matches!(refusal, iroh::endpoint::ConnectionError::ApplicationClosed(close)
            if close.reason.as_ref() == b"peer unavailable"));
        outside.endpoint().await.unwrap().close().await;
        tokio::try_join!(holder.shutdown(), neighbor.shutdown()).unwrap();
    }).await.expect("the holder must reject the direct request");
}

async fn retry_origin_after(first_get: FirstGet) {
    let directory = tempfile::tempdir().unwrap();
    let body = test_data(192 * 1024);
    let hash = BlakeHash::hash(&body);
    let origin = OriginServer::start_with(hash, body.clone(), Some(first_get)).await;
    let mut config = AssetClassConfig::new("chunks", directory.path());
    config.origin = Some(origin.config(1));
    let node = AssetClass::register(config).await.unwrap();
    let asset = node.indexed_chunk(hash, body.len() as u64);
    assert_eq!(asset.read_chunk().await.unwrap().as_ref(), body);
    assert_eq!(origin.gets(), 2);
    assert!(asset.is_cached());
    node.shutdown().await.unwrap();
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test]
async fn a_throttled_origin_get_is_retried() {
    retry_origin_after(FirstGet::Throttle).await;
}

#[tokio::test]
async fn an_interrupted_origin_body_is_retried_from_the_start() {
    retry_origin_after(FirstGet::Disconnect(64 * 1024)).await;
}

#[tokio::test]
async fn a_corrupt_origin_chunk_never_enters_the_cache() {
    let directory = tempfile::tempdir().unwrap();
    let hash = BlakeHash::hash(b"expected");
    let origin = OriginServer::start(hash, b"tampered".to_vec()).await;
    let mut config = AssetClassConfig::new("chunks", directory.path());
    config.origin = Some(origin.config(1));
    let node = AssetClass::register(config).await.unwrap();
    let asset = node.indexed_chunk(hash, 8);
    assert!(matches!(asset.read_chunk().await, Err(Error::OriginRejected { .. })));
    assert!(!asset.is_cached());
    assert_eq!(origin.gets(), 1);
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_indexed_chunks_use_the_direct_peer_path() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let directory = tempfile::tempdir().unwrap();
        let data = test_data(256 * 1024 * 1024);
        let chunks: Vec<_> = data.chunks(IMAGE_CHUNK_BYTES)
            .map(|data| (BlakeHash::hash(data), data)).collect();
        let s3 = OriginServer::start(chunks[0].0, chunks[0].1.to_vec()).await;
        let holder = peer_node_with_memory(&directory.path().join("holder"), "aws/us-west-2a", PeerReach::Local, None, 1024).await;
        let client = peer_node_with_memory(&directory.path().join("client"), "aws/us-west-2a", PeerReach::Local, Some(s3.config(4)), 1024).await;
        let holder_id = holder.node_id().unwrap();
        holder.set_peers(vec![cache_address(&client).await]);
        client.set_peers(vec![cache_address(&holder).await]);
        for &(hash, body) in &chunks {
            holder.0.cache.insert(hash, body.len() as u64, Bytes::copy_from_slice(body)).await.unwrap();
        }
        assert_eq!(holder.0.cache.complete_hashes().len(), chunks.len());
        for &(hash, _) in &chunks {
            wait_for_records(&client, hash, &[holder_id]).await;
        }

        let started = Instant::now();
        let pulls = futures::future::join_all(chunks.iter().map(|&(hash, data)| {
            let client = &client;
            async move { client.indexed_chunk(hash, data.len() as u64).read_chunk_reported().await }
        })).await;
        let elapsed = started.elapsed();
        for ((result, report), (_, expected)) in pulls.into_iter().zip(chunks) {
            assert_eq!(result.unwrap().as_ref(), expected);
            assert_eq!((report.peer_bytes, report.origin_bytes), (expected.len() as u64, 0));
        }
        assert!(s3.gets() == 0);
        let gbps = (data.len() as f64 * 8.0) / (elapsed.as_secs_f64() * 1e9);
        println!("Verified 256 MiB of indexed peer data in {elapsed:?} ({gbps:.2} Gbit/s)");
        tokio::try_join!(client.shutdown(), holder.shutdown()).unwrap();
    }).await.expect("verify a large peer fetch within 45 seconds");
}

// [itest -> dsn~xlb-foyer-cas~1]
#[tokio::test(flavor = "multi_thread")]
async fn restarted_cas_node_announces_and_serves_its_spilled_chunk() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let data = test_data(IMAGE_CHUNK_BYTES);
        let hash = BlakeHash::hash(&data);
        let origin = OriginServer::start(hash, data.clone()).await;
        let holder_path = dir.path().join("holder");
        let config = |path: &Path| {
            let mut config = AssetClassConfig::new("chunks", path);
            config.cas_memory_bytes = 160 << 20;
            config.cache_budget_bytes = 128 << 20;
            config.peer = Some(PeerConfig {
                bind_addr: BIND, relay: RelaySetting::Disabled, ..PeerConfig::default()
            });
            config
        };
        let mut source_config = config(&holder_path);
        source_config.origin = Some(origin.config(4));
        let first = AssetClass::register(source_config).await.unwrap();
        let bytes = first.indexed_chunk(hash, data.len() as u64).read_chunk().await.unwrap();
        assert_eq!(&bytes[..], &data);
        assert!(!holder_path.join(hash.to_hex()).exists());
        first.shutdown().await.unwrap();
        drop(first);

        let holder = AssetClass::register(config(&holder_path)).await.unwrap();
        let reader = AssetClass::register(config(&dir.path().join("reader"))).await.unwrap();
        let topology = NodeTopology {
            provider: "aws".into(), location: "aws/us-west-2a".into(),
            region: Some("us-west-2".into()), zone: Some("us-west-2a".into()), reach: PeerReach::Local,
        };
        holder.set_node_topology(Some(topology.clone()));
        reader.set_node_topology(Some(topology));
        let holder_id = holder.node_id().unwrap();
        holder.set_peers(vec![cache_address(&reader).await]);
        reader.set_peers(vec![cache_address(&holder).await]);
        wait_for_records(&reader, hash, &[holder_id]).await;
        let (body, report) = reader.indexed_chunk(hash, data.len() as u64).read_chunk_reported().await;
        assert_eq!(&body.unwrap()[..], &data);
        assert_eq!(report.peer_bytes, data.len() as u64);
        assert_eq!(report.origin_bytes, 0);
        tokio::try_join!(reader.shutdown(), holder.shutdown()).unwrap();
    }).await.unwrap();
}

async fn cache_address(node: &AssetClass) -> PeerAddr {
    loop {
        let addrs: Vec<_> = node.bound_addrs().into_iter().filter(|addr| addr.ip().is_loopback()).collect();
        if !addrs.is_empty() {
            return PeerAddr { node_id: node.node_id().unwrap(), addrs, relay_url: None };
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// [itest -> dsn~xlb-supervised-lifecycle~1,dsn~xlb-fork~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_node_reads_a_verified_chunk_with_no_bootstrap_peers() {
    let directory = tempfile::tempdir().unwrap();
    let data = test_data(96 * 1024);
    let hash = BlakeHash::hash(&data);
    let origin = OriginServer::start(hash, data.clone()).await;
    let mut config = AssetClassConfig::new("chunks", directory.path());
    config.cas_memory_bytes = 160 << 20;
    config.cache_budget_bytes = 128 << 20;
    config.origin = Some(origin.config(2));
    config.peer = Some(PeerConfig { bind_addr: BIND, relay: RelaySetting::Disabled, ..PeerConfig::default() });
    let node = AssetClass::register(config).await.unwrap();

    let (body, report) = node.indexed_chunk(hash, data.len() as u64).read_chunk_reported().await;
    assert_eq!(body.unwrap().as_ref(), data);
    assert_eq!((report.peer_bytes, report.origin_bytes), (0, data.len() as u64));
    assert_eq!(origin.gets(), 1);
    node.shutdown().await.unwrap();
}

// [itest -> dsn~xlb-supervised-lifecycle~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_last_node_serves_from_s3_and_learns_a_later_joiner() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let first = test_data(64 * 1024);
        let from_origin = test_data(80 * 1024);
        let from_joiner = test_data(96 * 1024);
        let origin_hash = BlakeHash::hash(&from_origin);
        let origin = OriginServer::start(origin_hash, from_origin.clone()).await;
        let start = async |name: &str, location: &str, reach, backing_store| {
            let mut config = AssetClassConfig::new("chunks", directory.path().join(name));
            config.cas_memory_bytes = 160 << 20;
            config.cache_budget_bytes = 128 << 20;
            config.origin = backing_store;
            config.peer = Some(PeerConfig { bind_addr: BIND, relay: RelaySetting::Disabled, ..PeerConfig::default() });
            let node = AssetClass::register(config).await.unwrap();
            node.set_node_topology(Some(NodeTopology {
                provider: "aws".to_owned(), region: None, zone: None, location: location.to_owned(), reach,
            }));
            node
        };

        let survivor = start("survivor", "aws/us-west-2a", PeerReach::Local, Some(origin.config(2))).await;
        let old = start("old", "aws/us-west-2b", PeerReach::Global, None).await;
        let first_hash = BlakeHash::hash(&first);
        old.0.cache.insert(first_hash, first.len() as u64, first.clone().into()).await.unwrap();
        old.set_peers(vec![cache_address(&survivor).await]);
        survivor.set_peers(vec![cache_address(&old).await]);
        wait_for_records(&survivor, first_hash, &[old.node_id().unwrap()]).await;
        let (body, report) = survivor.indexed_chunk(first_hash, first.len() as u64).read_chunk_reported().await;
        assert_eq!(body.unwrap().as_ref(), first);
        assert_eq!(report.peer_bytes, first.len() as u64);

        old.shutdown().await.unwrap();
        survivor.set_peers(Vec::new());
        let (body, report) = survivor.indexed_chunk(origin_hash, from_origin.len() as u64).read_chunk_reported().await;
        assert_eq!(body.unwrap().as_ref(), from_origin);
        assert_eq!(report.origin_bytes, from_origin.len() as u64);

        let joiner = start("joiner", "aws/us-west-2a", PeerReach::Local, None).await;
        let joiner_hash = BlakeHash::hash(&from_joiner);
        joiner.0.cache.insert(joiner_hash, from_joiner.len() as u64, from_joiner.clone().into()).await.unwrap();
        joiner.set_peers(vec![cache_address(&survivor).await]);
        survivor.set_peers(vec![cache_address(&joiner).await]);
        let joiner_id = joiner.node_id().unwrap();
        wait_for_records(&survivor, joiner_hash, &[joiner_id]).await;
        let (body, report) = survivor.indexed_chunk(joiner_hash, from_joiner.len() as u64).read_chunk_reported().await;
        assert_eq!(body.unwrap().as_ref(), from_joiner);
        assert_eq!(report.peers.get(&joiner_id.to_string()), Some(&(from_joiner.len() as u64)));
        assert_eq!(origin.gets(), 1);
        tokio::try_join!(survivor.shutdown(), joiner.shutdown()).unwrap();
    }).await.expect("the surviving node recovers peer sourcing without restarting");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicting_index_lengths_do_not_fail_an_unrelated_reader() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let directory = tempfile::tempdir().unwrap();
        let data = test_data(128 * 1024 + 23);
        let size = data.len() as u64;
        let hash = BlakeHash::hash(&data);
        let origin = OriginServer::start(hash, data.clone()).await;
        origin.pause(true);
        let mut config = AssetClassConfig::new("chunks", directory.path());
        config.cas_memory_bytes = 160 << 20;
        config.cache_budget_bytes = 128 << 20;
        config.origin = Some(origin.config(2));
        let node = AssetClass::register(config).await.unwrap();

        let invalid = node.indexed_chunk(hash, size - 1);
        let wrong_read = tokio::spawn(async move { invalid.read_chunk().await });
        while origin.gets() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let valid = node.indexed_chunk(hash, size);
        let correct_read = valid.read_chunk();
        tokio::pin!(correct_read);
        assert!(tokio::time::timeout(Duration::from_millis(200), &mut correct_read).await.is_err());
        origin.pause(false);
        assert!(wrong_read.await.unwrap().is_err());
        assert!(correct_read.await.is_ok_and(|body| body.as_ref() == data),
            "valid metadata must not inherit an error from the conflicting request");
        assert!(valid.read_chunk().await.is_ok_and(|body| body.as_ref() == data));
        node.shutdown().await.unwrap();
    }).await.expect("the correct read recovers after the conflicting request");
}

// [itest -> dsn~xlb-foyer-cas~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_damaged_cache_chunk_is_reloaded_from_signed_s3() {
    use std::os::unix::fs::FileExt;

    tokio::time::timeout(Duration::from_secs(25), async {
        let directory = tempfile::tempdir().unwrap();
        let data = test_data(IMAGE_CHUNK_BYTES);
        let hash = BlakeHash::hash(&data);
        let origin = OriginServer::start(hash, data.clone()).await;
        let start = || {
            let mut config = AssetClassConfig::new("chunks", directory.path());
            config.cas_memory_bytes = 160 << 20;
            config.cache_budget_bytes = 128 << 20;
            config.origin = Some(origin.config(2));
            AssetClass::register(config)
        };
        let node = start().await.unwrap();
        let size = data.len() as u64;
        assert_eq!(&node.indexed_chunk(hash, size).read_chunk().await.unwrap()[..], &data);
        node.shutdown().await.unwrap();
        drop(node);

        let device = directory.path().join(".foyer/whole-chunks");
        let signature = &data[..32];
        let mut damaged = false;
        for entry in std::fs::read_dir(device).unwrap() {
            let path = entry.unwrap().path();
            let block = std::fs::read(&path).unwrap();
            if let Some(offset) = block.windows(signature.len()).position(|bytes| bytes == signature) {
                std::fs::OpenOptions::new().write(true).open(path).unwrap()
                    .write_all_at(&[signature[0] ^ 1], offset as u64).unwrap();
                damaged = true;
            }
        }
        assert!(damaged, "the complete payload was written to the device before the fault");

        let recovered = start().await.unwrap();
        let asset = recovered.indexed_chunk(hash, size);
        let (payload, report) = asset.read_chunk_reported().await;
        assert_eq!(&payload.unwrap()[..], &data);
        assert_eq!(report.origin_bytes, size);
        let (_, warm) = asset.read_chunk_reported().await;
        assert_eq!(warm.origin_bytes, 0);
        recovered.shutdown().await.unwrap();
    }).await.expect("recover verified data after cache disk corruption");
}

// [itest -> dsn~xlb-chunk-discovery~1,dsn~xlb-foyer-cas~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mixed_cluster_reads_from_the_global_holder_after_local_loss() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let directory = tempfile::tempdir().unwrap();
        let data = test_data(IMAGE_CHUNK_BYTES);
        let hash = BlakeHash::hash(&data);
        let origin = OriginServer::start(hash, data.clone()).await;
        let s3_data = test_data(IMAGE_CHUNK_BYTES / 2 + 17);
        let s3_hash = BlakeHash::hash(&s3_data);
        let fallback = OriginServer::start(s3_hash, s3_data.clone()).await;
        let node = async |name: &str, location: &str, reach, budget: (usize, u64), source: &OriginServer| {
            let mut config = AssetClassConfig::new("chunks", directory.path().join(name));
            config.cas_memory_bytes = budget.0 << 20;
            config.cache_budget_bytes = budget.1 << 20;
            config.origin = Some(source.config(4));
            config.peer = Some(PeerConfig { bind_addr: BIND, relay: RelaySetting::Disabled, ..PeerConfig::default() });
            config.request_limits = Some(RequestLimits::new(2.try_into().unwrap(), 1.try_into().unwrap(), 1.try_into().unwrap()).unwrap());
            let class = AssetClass::register(config).await.unwrap();
            class.set_node_topology(Some(NodeTopology {
                provider: location.split('/').next().unwrap().into(), location: location.into(), region: None, zone: None, reach,
            }));
            class
        };
        let local = node("local", "aws/us-west-2a", PeerReach::Local, (160, 128), &origin).await;
        let global = node("global", "nebius/eu-north1", PeerReach::Global, (256, 256), &origin).await;
        let outside = node("outside", "aws/us-east-1a", PeerReach::Local, (160, 128), &origin).await;
        let reader = node("reader", "aws/us-west-2a", PeerReach::Local, (160, 128), &fallback).await;
        for holder in [&local, &global, &outside] {
            let payload = holder.indexed_chunk(hash, data.len() as u64).read_chunk().await.unwrap();
            assert_eq!(&payload[..], data);
        }
        let reader_id = reader.node_id().unwrap();
        let global_id = global.node_id().unwrap();
        let local_id = local.node_id().unwrap();
        reader.set_peers(vec![cache_address(&local).await, cache_address(&global).await]);
        local.set_peers(vec![cache_address(&reader).await]);
        global.set_peer_sets(vec![cache_address(&reader).await], vec![reader_id]);
        wait_for_records(&reader, hash, &[local_id, global_id]).await;
        local.shutdown().await.unwrap();

        fallback.pause(true);
        let s3_asset = reader.indexed_chunk(s3_hash, s3_data.len() as u64);
        let s3_read = tokio::spawn(async move { s3_asset.read_chunk_reported().await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !reader.s3_request_stats().is_some_and(|stats| stats.active_chunks == 1) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("the indexed S3 read reaches the paused origin");
        let reads = futures::future::join_all((0..4).map(|_| {
            let asset = reader.indexed_chunk(hash, data.len() as u64);
            async move { asset.read_chunk_reported().await }
        })).await;
        let mut saw_global = false;
        for (result, report) in reads {
            assert_eq!(&result.unwrap()[..], data);
            assert_eq!(report.origin_bytes, 0);
            if report.peer_bytes != 0 {
                assert_eq!(report.peers.len(), 1);
                assert_eq!(report.peers.get(&global_id.to_string()), Some(&(data.len() as u64)));
                saw_global = true;
            }
        }
        assert!(saw_global);
        let (warm, report) = reader.indexed_chunk(hash, data.len() as u64).read_chunk_reported().await;
        assert_eq!(&warm.unwrap()[..], data);
        assert_eq!((report.peer_bytes, report.origin_bytes), (0, 0));
        assert!(!s3_read.is_finished(), "peer and cache reads finish while S3 reads are pending");
        fallback.pause(false);
        let (body, report) = s3_read.await.unwrap();
        assert_eq!(&body.unwrap()[..], s3_data);
        assert_eq!((report.peer_bytes, report.origin_bytes), (0, s3_data.len() as u64));
        assert_eq!(reader.s3_request_stats().unwrap().peak_chunks, 1);
        tokio::try_join!(reader.shutdown(), global.shutdown(), outside.shutdown()).unwrap();
    }).await.expect("global peer fallback after zone-local loss");
}
