//! Faults at the peer protocol boundary, with real smol-kv advertisements.

#[path = "../tests/origin_server/mod.rs"]
mod origin_server;

use std::{
    net::SocketAddr,
    path::Path,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::Duration,
};

use futures::StreamExt;
use iroh::{Endpoint, EndpointAddr, RelayMode, TransportAddr, address_lookup::MemoryLookup, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, api::TempTag, protocol::GetRequest, provider::events::{EventMask, EventSender, ProviderMessage, RequestMode}, store::mem::MemStore};
use iroh_gossip::Gossip;
use iroh_smol_kv::{Client, Config};
use tokio::time::Instant;
use tokio_util::task::AbortOnDropHandle;
use xlb_core::{PeerReach, image::IMAGE_CHUNK_BYTES};

use super::*;
use crate::{AssetClass, AssetClassConfig, NodeTopology, PeerAddr, PeerConfig, RelaySetting, RequestLimits, discovery::discovery_topic};
use origin_server::{OriginServer, RangeBehavior, test_data};

const BIND: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 0));
const RANGE_BYTES: u64 = 128 * 1024;

struct ProtocolPeer {
    endpoint: Endpoint,
    router: Router,
    kv: Client,
    gets: Arc<AtomicUsize>,
    _events: AbortOnDropHandle<()>,
    _blobs: Vec<TempTag>,
}

impl ProtocolPeer {
    async fn start(receiver: &AssetClass, blobs: &[&[u8]], stall_after: Option<usize>) -> Self {
        let receiver_id = receiver.node_id().unwrap();
        let address = EndpointAddr::from_parts(receiver_id, receiver.bound_addrs().into_iter().map(TransportAddr::Ip));
        let endpoint = Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
            .address_lookup(MemoryLookup::from_endpoint_info([address]))
            .clear_ip_transports().bind_addr(BIND).unwrap().bind().await.unwrap();
        let store = MemStore::new();
        let mut tags = Vec::new();
        for blob in blobs {
            tags.push(store.blobs().add_slice(blob).temp_tag().await.unwrap());
        }
        let (events, mut requests) = EventSender::channel(16, EventMask { get: RequestMode::Intercept, ..EventMask::default() });
        let gets = Arc::new(AtomicUsize::new(0));
        let observed = gets.clone();
        let serve = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Some(request) = requests.recv().await {
                if let ProviderMessage::GetRequestReceived(request) = request {
                    let previous = observed.fetch_add(1, Ordering::Relaxed);
                    if stall_after.is_some_and(|allowed| previous >= allowed) {
                        held.push(request.tx);
                    } else {
                        request.tx.send(Ok(())).await.ok();
                    }
                }
            }
        });
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, Some(events)))
            .accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let topic = gossip.subscribe_and_join(discovery_topic(), vec![receiver_id]).await.unwrap();
        let kv = Client::local(topic, Config::default());
        Self { endpoint, router, kv, gets, _events: AbortOnDropHandle::new(serve), _blobs: tags }
    }

    fn address(&self) -> PeerAddr {
        PeerAddr { node_id: self.endpoint.id(), addrs: self.endpoint.bound_sockets(), relay_url: None }
    }

    async fn advertise(&self, hash: BlakeHash, location: &str) {
        let node = self.endpoint.id();
        let key = format!("CAS/{hash}/{location}/{node}", hash = hash.to_hex());
        let value = serde_json::to_vec(&self.endpoint.addr().addrs).unwrap();
        self.kv.write(self.endpoint.secret_key().clone()).put(key, value).await.unwrap();
    }
}

async fn receiver(path: &Path, origin: &OriginServer, peers: Vec<PeerAddr>) -> AssetClass {
    let mut config = AssetClassConfig::new("chunks", path);
    config.origin = Some(origin.config(RANGE_BYTES, 4));
    config.peer = Some(PeerConfig { bind_addr: BIND, relay: RelaySetting::Disabled, ..PeerConfig::default() });
    let node = AssetClass::register(config).await.unwrap();
    node.set_node_topology(Some(NodeTopology {
        provider: "aws".into(), location: "aws/us-west-2a".into(),
        region: Some("us-west-2".into()), zone: Some("us-west-2a".into()), reach: PeerReach::Local,
    }));
    node.set_peers(peers);
    node
}

async fn wait_for_records(receiver: &AssetClass, hash: BlakeHash, expected: &[NodeId]) {
    let records = receiver.0.peers.as_ref().unwrap().providers(to_iroh(&hash));
    tokio::pin!(records);
    let mut seen = std::collections::HashSet::new();
    while let Some(record) = records.next().await {
        if let ProviderEvent::Available { address, .. } = record {
            seen.insert(address.id);
        }
        if expected.iter().all(|id| seen.contains(id)) {
            return;
        }
    }
    panic!("the peer directory closed before the advertised providers arrived");
}

async fn check_mixed_tail_group(corrupt: bool, disconnect: bool) {
    let dir = tempfile::tempdir().unwrap();
    let tail = 2 * RANGE_BYTES;
    let data = test_data(tail as usize + 10_774);
    let hash = BlakeHash::hash(&data);
    let behavior = RangeBehavior {
        corrupt_at: corrupt.then_some(tail as usize + 3),
        ..RangeBehavior::default()
    };
    let s3 = OriginServer::start_with(hash, data.clone(), behavior).await;
    let receiver = receiver(dir.path(), &s3, Vec::new()).await;
    let peer = ProtocolPeer::start(&receiver, &[&data], Some(1)).await;
    receiver.set_peers(vec![peer.address()]);
    peer.advertise(hash, "aws%2Fus-west-2a").await;
    wait_for_records(&receiver, hash, &[peer.endpoint.id()]).await;

    let asset = receiver.asset(hash);
    let fetch = asset.fetch_to_file_reported(None);
    let (result, report) = if disconnect {
        let cut_connection = async {
            while peer.gets.load(Ordering::Relaxed) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            peer.endpoint.close().await;
        };
        tokio::join!(fetch, cut_connection).0
    } else { fetch.await };
    let size = data.len() as u64;
    let peer_bytes = 534;
    let tail_range = (tail, size - peer_bytes);
    let ranges = s3.object_ranges();
    assert_eq!(ranges.iter().copied().filter(|&(_, end)| end > tail).collect::<Vec<_>>(), [tail_range]);
    if corrupt {
        assert!(matches!(result, Err(Error::OriginRejected { hash: rejected, .. }) if rejected == hash), "{result:?}");
        let stored = receiver.file_store().unwrap().api().blobs().observe(to_iroh(&hash)).await.unwrap();
        let peer_tail = ChunkRanges::bytes(tail_range.1..size);
        assert!(peer_tail.is_subset(&stored.ranges), "the peer bytes remain verified");
        let rejected = ChunkRanges::bytes(tail_range.0..tail_range.1);
        assert!(!stored.ranges.intersects(&rejected), "corrupt bytes must remain unverified");
        assert!(!asset.is_cached());
    } else {
        let path = result.expect("combine the peer's tail with verified S3 bytes");
        assert_eq!(std::fs::read(path).unwrap(), data);
        assert_eq!(ranges, [(0, RANGE_BYTES), (RANGE_BYTES, tail), tail_range]);
        assert_eq!(report.peer_bytes, peer_bytes);
        assert_eq!(report.peers, std::collections::BTreeMap::from([(peer.endpoint.id().to_string(), peer_bytes)]));
        assert_eq!(report.origin_bytes, size - peer_bytes);
    }
    receiver.shutdown().await;
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread")]
async fn s3_completes_a_partial_group_without_downloading_the_peer_bytes_again() {
    tokio::time::timeout(Duration::from_secs(25), check_mixed_tail_group(false, false)).await.unwrap();
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread")]
async fn s3_corruption_in_a_group_partially_received_from_a_peer_is_rejected() {
    tokio::time::timeout(Duration::from_secs(25), check_mixed_tail_group(true, false)).await.unwrap();
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test(flavor = "multi_thread")]
async fn an_in_flight_peer_disconnect_does_not_discard_verified_bytes() {
    tokio::time::timeout(Duration::from_secs(25), check_mixed_tail_group(false, true)).await.unwrap();
}

// [utest -> dsn~xlb-chunk-discovery~1]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_indexed_chunks_use_the_direct_peer_path() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let directory = tempfile::tempdir().unwrap();
        let data = test_data(100 * 1024 * 1024);
        let chunks: Vec<_> = data.chunks(IMAGE_CHUNK_BYTES)
            .map(|data| (BlakeHash::hash(data), data)).collect();
        let source = directory.path().join("source");
        std::fs::create_dir(&source).unwrap();
        for (hash, bytes) in &chunks {
            std::fs::write(source.join(hash.to_hex()), bytes).unwrap();
        }
        let s3 = OriginServer::start(chunks[0].0, chunks[0].1.to_vec()).await;
        let holder = receiver(&directory.path().join("holder"), &s3, Vec::new()).await;
        let client = receiver(&directory.path().join("client"), &s3, Vec::new()).await;
        let holder_id = holder.node_id().unwrap();
        holder.set_peer_sets(Vec::new(), vec![client.node_id().unwrap()]);
        client.set_peers(vec![PeerAddr { node_id: holder_id, addrs: holder.bound_addrs(), relay_url: None }]);
        holder.pin_tree("image", chunks.iter().map(|&(hash, _)| hash)).await.unwrap();
        let seeded = holder.seed_tree_from_dirs("image", &[source]).await.unwrap();
        assert_eq!(seeded.ingested, chunks.len() as u64);
        for &(hash, _) in &chunks {
            wait_for_records(&client, hash, &[holder_id]).await;
        }

        let started = Instant::now();
        let pulls = futures::future::join_all(chunks.iter().map(|&(hash, data)| {
            let client = &client;
            async move { client.indexed_chunk(hash, data.len() as u64).fetch_to_file_reported(None).await }
        })).await;
        let elapsed = started.elapsed();
        for ((result, report), (_, expected)) in pulls.into_iter().zip(chunks) {
            assert_eq!(std::fs::read(result.unwrap()).unwrap(), expected);
            assert_eq!((report.peer_bytes, report.origin_bytes), (expected.len() as u64, 0));
        }
        assert!(s3.seen().is_empty());
        let gbps = (data.len() as f64 * 8.0) / (elapsed.as_secs_f64() * 1e9);
        println!("Verified 100 MiB of indexed peer data in {elapsed:?} ({gbps:.2} Gbit/s)");
        tokio::join!(client.shutdown(), holder.shutdown());
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
            config.cas_memory_bytes = Some(160 << 20);
            config.cache_budget_bytes = 128 << 20;
            config.peer = Some(PeerConfig {
                bind_addr: BIND, relay: RelaySetting::Disabled, ..PeerConfig::default()
            });
            config
        };
        let mut source_config = config(&holder_path);
        source_config.origin = Some(origin.config(RANGE_BYTES, 4));
        let first = AssetClass::register(source_config).await.unwrap();
        let bytes = first.indexed_chunk(hash, data.len() as u64).read_chunk().await.unwrap();
        assert_eq!(&bytes[..], &data);
        assert!(!holder_path.join(hash.to_hex()).exists());
        first.shutdown().await;
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
        holder.set_peer_sets(Vec::new(), vec![reader.node_id().unwrap()]);
        reader.set_peers(vec![PeerAddr { node_id: holder_id, addrs: holder.bound_addrs(), relay_url: None }]);
        wait_for_records(&reader, hash, &[holder_id]).await;
        let (body, report) = reader.indexed_chunk(hash, data.len() as u64).read_chunk_reported().await;
        assert_eq!(&body.unwrap()[..], &data);
        assert_eq!(report.peer_bytes, data.len() as u64);
        assert_eq!(report.origin_bytes, 0);
        tokio::join!(reader.shutdown(), holder.shutdown());
    }).await.unwrap();
}

fn cache_address(node: &AssetClass) -> PeerAddr {
    PeerAddr { node_id: node.node_id().unwrap(), addrs: node.bound_addrs(), relay_url: None }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicting_index_lengths_do_not_fail_an_unrelated_reader() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let directory = tempfile::tempdir().unwrap();
        let data = test_data(RANGE_BYTES as usize + 23);
        let size = data.len() as u64;
        let hash = BlakeHash::hash(&data);
        let origin = OriginServer::start_with(hash, data.clone(), RangeBehavior {
            pause_tail: true, ..RangeBehavior::default()
        }).await;
        origin.pause_object_ranges(true);
        let mut config = AssetClassConfig::new("chunks", directory.path());
        config.cas_memory_bytes = Some(160 << 20);
        config.cache_budget_bytes = 128 << 20;
        config.origin = Some(origin.config(RANGE_BYTES, 2));
        let node = AssetClass::register(config).await.unwrap();

        let invalid = node.indexed_chunk(hash, size - 1);
        let wrong_read = tokio::spawn(async move { invalid.read_chunk().await });
        while origin.object_ranges().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let valid = node.indexed_chunk(hash, size);
        let correct_read = valid.read_chunk();
        tokio::pin!(correct_read);
        assert!(tokio::time::timeout(Duration::from_millis(200), &mut correct_read).await.is_err());
        origin.pause_object_ranges(false);
        assert!(wrong_read.await.unwrap().is_err());
        assert!(correct_read.await.is_ok_and(|body| body.as_ref() == data),
            "valid metadata must not inherit an error from the conflicting request");
        assert!(valid.read_chunk().await.is_ok_and(|body| body.as_ref() == data));
        node.shutdown().await;
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
            config.cas_memory_bytes = Some(160 << 20);
            config.cache_budget_bytes = 128 << 20;
            config.origin = Some(origin.config(RANGE_BYTES, 2));
            AssetClass::register(config)
        };
        let node = start().await.unwrap();
        let size = data.len() as u64;
        assert_eq!(&node.indexed_chunk(hash, size).read_chunk().await.unwrap()[..], &data);
        node.shutdown().await;
        drop(node);

        let device = directory.path().join(".foyer/chunks");
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
        recovered.shutdown().await;
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
        let fallback = OriginServer::start_with(s3_hash, s3_data.clone(), RangeBehavior { pause_tail: true, ..Default::default() }).await;
        let node = async |name: &str, location: &str, reach, budget: (usize, u64), source: &OriginServer| {
            let mut config = AssetClassConfig::new("chunks", directory.path().join(name));
            config.cas_memory_bytes = Some(budget.0 << 20);
            config.cache_budget_bytes = budget.1 << 20;
            config.origin = Some(source.config(RANGE_BYTES, 4));
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
        let outside_id = outside.node_id().unwrap();
        reader.set_peers(vec![cache_address(&local), cache_address(&global)]);
        local.set_peers(vec![cache_address(&reader)]);
        global.set_peer_sets(vec![cache_address(&outside)], vec![reader_id, outside_id]);
        outside.set_peer_sets(vec![cache_address(&global)], vec![reader_id, global_id]);
        wait_for_records(&reader, hash, &[local_id, global_id, outside_id]).await;
        local.shutdown().await;

        fallback.pause_object_ranges(true);
        let s3_asset = reader.indexed_chunk(s3_hash, s3_data.len() as u64);
        let s3_read = tokio::spawn(async move { s3_asset.read_chunk_reported().await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !reader.s3_request_stats().is_some_and(|stats| stats.active_ranges == 1) {
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
        fallback.pause_object_ranges(false);
        let (body, report) = s3_read.await.unwrap();
        assert_eq!(&body.unwrap()[..], s3_data);
        assert_eq!((report.peer_bytes, report.origin_bytes), (0, s3_data.len() as u64));
        assert_eq!(reader.s3_request_stats().unwrap().peak_ranges, 1);
        tokio::join!(reader.shutdown(), global.shutdown(), outside.shutdown());
    }).await.expect("global peer fallback after zone-local loss");
}

// [utest -> dsn~xlb-chunk-discovery~1]
#[tokio::test(flavor = "multi_thread")]
async fn a_stuck_local_holder_times_out_and_another_holder_serves_the_data() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let dir = tempfile::tempdir().unwrap();
        let first = test_data(384 * 1024);
        let second = test_data(256 * 1024);
        let first_hash = BlakeHash::hash(&first);
        let second_hash = BlakeHash::hash(&second);
        let data = [first.as_slice(), second.as_slice()];
        let s3 = OriginServer::start(first_hash, first.clone()).await;
        let receiver = receiver(dir.path(), &s3, Vec::new()).await;
        let (local, global) = tokio::join!(ProtocolPeer::start(&receiver, &data, Some(0)), ProtocolPeer::start(&receiver, &data, None));
        receiver.set_peers(vec![global.address(), local.address()]);
        let ids = [local.endpoint.id(), global.endpoint.id()];
        for hash in [first_hash, second_hash] {
            local.advertise(hash, "aws%2Fus-west-2a").await;
            global.advertise(hash, "nebius%2Feu-north1").await;
        }
        wait_for_records(&receiver, first_hash, &ids).await;
        wait_for_records(&receiver, second_hash, &ids).await;

        let started = Instant::now();
        let (result, report) = receiver.indexed_chunk(first_hash, first.len() as u64).fetch_to_file_reported(None).await;
        assert_eq!(std::fs::read(result.expect("fetch from alternate peer")).unwrap(), first);
        assert_eq!(local.gets.load(Ordering::Relaxed), 1, "try the local holder once before the global holder");
        assert_eq!(global.gets.load(Ordering::Relaxed), 1, "the entire indexed chunk takes one request");
        assert!(started.elapsed() >= Duration::from_secs(5));
        assert!(started.elapsed() < Duration::from_secs(10), "the stalled attempt must end at five seconds");
        assert_eq!((report.peer_bytes, report.origin_bytes), (first.len() as u64, 0));
        assert!(s3.seen().is_empty(), "the alternate peer supplied the entire chunk");

        let path = receiver.indexed_chunk(second_hash, second.len() as u64).fetch_to_file().await.expect("fetch another chunk from the healthy holder");
        assert_eq!(std::fs::read(path).unwrap(), second);
        assert_eq!(local.gets.load(Ordering::Relaxed), 1, "the failed holder is excluded across hashes");
        receiver.shutdown().await;
    }).await.expect("the stuck peer cannot stall the entire fetch");
}

// [utest -> dsn~xlb-chunk-discovery~1]
#[tokio::test(flavor = "multi_thread")]
async fn an_offline_announced_holder_falls_back_to_signed_s3() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let dir = tempfile::tempdir().unwrap();
        let data = test_data(512 * 1024 + 17);
        let hash = BlakeHash::hash(&data);
        let s3 = OriginServer::start(hash, data.clone()).await;
        let receiver = receiver(dir.path(), &s3, Vec::new()).await;
        let gone = ProtocolPeer::start(&receiver, &[&data], None).await;
        receiver.set_peers(vec![gone.address()]);
        gone.advertise(hash, "aws%2Fus-west-2a").await;
        wait_for_records(&receiver, hash, &[gone.endpoint.id()]).await;
        gone.router.shutdown().await.unwrap();
        wait_for_records(&receiver, hash, &[gone.endpoint.id()]).await;

        let (result, report) = receiver.indexed_chunk(hash, data.len() as u64).fetch_to_file_reported(None).await;
        assert_eq!(std::fs::read(result.expect("S3 replaces failed peer")).unwrap(), data);
        assert_eq!((report.peer_bytes, report.origin_bytes), (0, data.len() as u64));
        let ranges = s3.object_ranges();
        assert!(ranges.windows(2).all(|ranges| ranges[0].1 == ranges[1].0));
        assert_eq!(ranges.first().unwrap().0, 0);
        assert_eq!(ranges.last().unwrap().1, data.len() as u64);
        receiver.shutdown().await;
    }).await.expect("a stale provider record cannot prevent S3 fallback");
}

// [utest -> dsn~xlb-chunk-discovery~1]
#[tokio::test(flavor = "multi_thread")]
async fn an_incomplete_chunk_serves_its_verified_range_before_advertising() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let dir = tempfile::tempdir().unwrap();
        let data = test_data(375 * 1024);
        let hash = BlakeHash::hash(&data);
        let s3 = OriginServer::start(hash, data.clone()).await;
        s3.pause_object_ranges(true);
        let receiver = receiver(dir.path(), &s3, Vec::new()).await;
        let asset = receiver.asset(hash);
        let download = tokio::spawn(async move { asset.fetch_to_file().await });
        while s3.object_ranges().iter().all(|&(_, end)| end == data.len() as u64) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let records = receiver.0.peers.as_ref().unwrap().providers(to_iroh(&hash));
        tokio::pin!(records);
        assert!(matches!(records.next().await, Some(ProviderEvent::CurrentDone)));
        assert!(!receiver.asset(hash).is_cached());

        let reader = Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
            .clear_ip_transports().bind_addr(BIND).unwrap().bind().await.unwrap();
        receiver.set_peer_sets(Vec::new(), vec![reader.id()]);
        let address = EndpointAddr::from_parts(receiver.node_id().unwrap(), receiver.bound_addrs().into_iter().map(TransportAddr::Ip));
        let connection = reader.connect(address, iroh_blobs::ALPN).await.unwrap();
        let store = MemStore::new();
        let tail = (data.len() as u64 - 1) / CHUNK_GROUP_BYTES * CHUNK_GROUP_BYTES;
        let ranges = ChunkRanges::bytes(tail..data.len() as u64);
        let blob = to_iroh(&hash);
        store.remote().execute_get(connection, GetRequest::blob_ranges(blob, ranges.clone())).await.unwrap();
        assert_eq!(store.blobs().export_bao(blob, ranges).data_to_vec().await.unwrap(), data[tail as usize..]);
        assert!(!download.is_finished());

        s3.pause_object_ranges(false);
        let path = download.await.unwrap().expect("finish the chunk before advertising");
        assert_eq!(std::fs::read(path).unwrap(), data);
        assert!(matches!(records.next().await, Some(ProviderEvent::Available { address, .. }) if Some(address.id) == receiver.node_id()));
        reader.close().await;
        receiver.shutdown().await;
    }).await.expect("the CAS key is published only after the data verifies");
}
