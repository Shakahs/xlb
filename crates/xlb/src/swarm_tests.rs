//! Whole CAS chunk transfers with real p2panda nodes and a signed S3 origin.

#[path = "../tests/origin_server/mod.rs"]
mod origin_server;

use std::{net::SocketAddr, path::Path, time::Duration};

use tokio::time::Instant;
use xlb_core::{PeerReach, image::IMAGE_CHUNK_BYTES};

use super::*;
use origin_server::{FirstGet, OriginServer, test_data};

const BIND: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 0));

#[derive(Clone, Debug, Default)]
struct MetricCapture(std::sync::Arc<std::sync::Mutex<Vec<(String, std::collections::BTreeMap<String, String>, f64)>>>);

impl opentelemetry_sdk::metrics::exporter::PushMetricExporter for MetricCapture {
    async fn export(&self, resource: &opentelemetry_sdk::metrics::data::ResourceMetrics) -> opentelemetry_sdk::error::OTelSdkResult {
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
        let mut captured = self.0.lock().unwrap();
        captured.clear();
        for metric in resource.scope_metrics().flat_map(|scope| scope.metrics()) {
            let mut add = |attributes: Vec<_>, value| captured.push((metric.name().to_owned(), attributes.into_iter()
                .map(|item: &opentelemetry::KeyValue| (item.key.as_str().to_owned(), item.value.as_str().into_owned())).collect(), value));
            match metric.data() {
                AggregatedMetrics::I64(MetricData::Sum(sum)) => for point in sum.data_points() { add(point.attributes().collect(), point.value() as f64); },
                AggregatedMetrics::U64(MetricData::Sum(sum)) => for point in sum.data_points() { add(point.attributes().collect(), point.value() as f64); },
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => for point in histogram.data_points() { add(point.attributes().collect(), point.count() as f64); },
                _ => {},
            }
        }
        Ok(())
    }
    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult { Ok(()) }
    fn shutdown_with_timeout(&self, _: Duration) -> opentelemetry_sdk::error::OTelSdkResult { Ok(()) }
    fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality { opentelemetry_sdk::metrics::Temporality::Cumulative }
}

fn metric_provider() -> (opentelemetry_sdk::metrics::SdkMeterProvider, MetricCapture) {
    let capture = MetricCapture::default();
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().with_periodic_exporter(capture.clone()).build();
    opentelemetry::global::set_meter_provider(provider.clone());
    (provider, capture)
}

impl MetricCapture {
    fn has(&self, name: &str, labels: &[(&str, &str)], minimum: f64) -> bool {
        self.0.lock().unwrap().iter().any(|(metric, attributes, value)| metric == name && *value >= minimum
            && labels.iter().all(|(key, value)| attributes.get(*key).is_some_and(|actual| actual == value)))
    }
}

// [itest -> dsn~xlb-connection-transfer-metrics~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run as the separate s3_telemetry_test Bazel action"]
async fn s3_metrics_report_active_requests_and_response_rates() {
    let (provider, capture) = metric_provider();
    let directory = tempfile::tempdir().unwrap();
    let body = test_data(64 * 1024);
    let hash = BlakeHash::hash(&body);
    let origin = OriginServer::start(hash, body.clone()).await;
    origin.pause(true);
    let mut config = AssetClassConfig::new("metrics", directory.path());
    config.origin = Some(origin.config(1));
    let node = AssetClass::register(config).await.unwrap();
    let asset = node.indexed_chunk(hash, body.len() as u64);
    let read = tokio::spawn(async move { asset.read_chunk().await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while origin.gets() == 0 { tokio::task::yield_now().await; }
    }).await.unwrap();
    provider.force_flush().unwrap();
    assert!(capture.has("opendal.http.executing", &[], 1.0), "OpenDAL must expose active S3 HTTP requests");
    assert!(capture.has("xlb.s3.requests.active", &[], 1.0), "S3 requests must remain active through response streaming");
    origin.pause(false);
    assert_eq!(read.await.unwrap().unwrap().as_ref(), body);
    provider.force_flush().unwrap();
    assert!(capture.has("opendal.http.response.bytes_rate", &[], 1.0), "OpenDAL must report response throughput");
    assert!(capture.has("xlb.transfer.bytes", &[("source", "s3"), ("direction", "download")], body.len() as f64));
    node.shutdown().await.unwrap();
}

// [itest -> dsn~xlb-connection-transfer-metrics~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run as the separate peer_telemetry_test Bazel action"]
async fn peer_metrics_report_connections_and_transferred_bytes() {
    let (provider, capture) = metric_provider();
    let directory = tempfile::tempdir().unwrap();
    let holder = peer_node(&directory.path().join("holder"), "aws/region", PeerReach::Local, None).await;
    let reader = peer_node(&directory.path().join("reader"), "aws/region", PeerReach::Local, None).await;
    let body = Bytes::from_static(b"peer transfer metric payload");
    let hash = BlakeHash::hash(&body);
    cache_data(&holder, hash, body.clone()).await;
    let holder_addr = cache_address(&holder).await;
    holder.set_peers(vec![cache_address(&reader).await]);
    reader.set_peers(vec![holder_addr.clone()]);
    wait_for_records(&reader, hash, &[holder_addr.node_id]).await;
    assert_eq!(reader.indexed_chunk(hash, body.len() as u64).read_chunk().await.unwrap(), body);
    provider.force_flush().unwrap();
    assert!(capture.has("xlb.peer.connections.active", &[("direction", "outgoing")], 1.0));
    assert!(capture.has("xlb.peer.connections.active", &[("direction", "incoming")], 1.0));
    for direction in ["download", "upload"] {
        assert!(capture.has("xlb.transfer.bytes", &[("source", "peer"), ("direction", direction)], body.len() as f64));
    }
    tokio::try_join!(reader.shutdown(), holder.shutdown()).unwrap();
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_limit_is_shared_across_asset_classes() {
    let directory = tempfile::tempdir().unwrap();
    let bodies = [Bytes::from_static(b"first chunk"), Bytes::from_static(b"second chunk")];
    let entries: Vec<_> = bodies.iter().map(|body| (BlakeHash::hash(body), body.clone())).collect();
    let origin = OriginServer::start_many(entries.clone()).await;
    origin.pause(true);
    let shared = crate::ChunkAdmission::new(1);
    let mut nodes = Vec::new();
    for name in ["first", "second"] {
        let mut config = AssetClassConfig::new(name, directory.path().join(name));
        config.origin = Some(origin.config(2));
        config.chunk_limit = Some(shared.clone());
        nodes.push(AssetClass::register(config).await.unwrap());
    }
    let pulls: Vec<_> = nodes.iter().zip(&entries).map(|(node, (hash, body))| {
        let chunk = node.indexed_chunk(*hash, body.len() as u64);
        tokio::spawn(async move { chunk.read_chunk().await })
    }).collect();
    tokio::time::timeout(Duration::from_secs(10), async {
        while origin.gets() == 0 { tokio::task::yield_now().await; }
    }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(origin.gets(), 1, "only one class may download at a time");
    origin.pause(false);
    for pull in pulls { assert!(pull.await.unwrap().is_ok()); }
    assert_eq!(origin.gets(), 2);
    for node in nodes { node.shutdown().await.unwrap(); }
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
struct PacedProvider {
    body: Bytes,
    sent_prefix: Arc<tokio::sync::Notify>,
    stall_after_prefix: bool,
}

impl iroh::protocol::ProtocolHandler for PacedProvider {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> std::result::Result<(), iroh::protocol::AcceptError> {
        let (mut send, mut receive) = connection.accept_bi().await?;
        let hash = BlakeHash::hash(&self.body);
        assert_eq!(receive.read_to_end(32).await.unwrap(), hash.as_bytes());
        let third = self.body.len() / 3;
        send.write_chunk(self.body.slice(..third)).await.unwrap();
        self.sent_prefix.notify_one();
        if !self.stall_after_prefix {
            tokio::time::sleep(Duration::from_secs(3)).await;
            send.write_chunk(self.body.slice(third..2 * third)).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
            send.write_chunk(self.body.slice(2 * third..)).await.unwrap();
            send.finish().unwrap();
        }
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

fn selected_path_uses_bbr(connection: &iroh::endpoint::Connection) -> bool {
    connection.paths().iter().find(|path| path.is_selected())
        .and_then(|path| connection.congestion_state(path.id()))
        .is_some_and(|controller| controller.into_any().is::<noq_proto::congestion::Bbr3>())
}

#[derive(Debug)]
struct CheckPeerController;

impl iroh::protocol::ProtocolHandler for CheckPeerController {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> std::result::Result<(), iroh::protocol::AcceptError> {
        let (mut send, mut receive) = connection.accept_bi().await?;
        assert_eq!(receive.read_to_end(1).await.unwrap(), [0]);
        let uses_bbr = u8::from(selected_path_uses_bbr(&connection));
        send.write_all(&[uses_bbr]).await.unwrap();
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xlb_endpoints_use_bbr_for_both_senders() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let directory = tempfile::tempdir().unwrap();
        let locality = "aws/us-west-2a";
        let reader = peer_node(&directory.path().join("reader"), locality, PeerReach::Local, None).await;
        let holder = peer_node(&directory.path().join("holder"), locality, PeerReach::Local, None).await;
        let holder_address = cache_address(&holder).await;
        holder.set_peers(vec![cache_address(&reader).await]);
        reader.set_peers(vec![holder_address.clone()]);
        let body = Bytes::from_static(b"BBR controller test inventory");
        let hash = BlakeHash::hash(&body);
        cache_data(&holder, hash, body).await;
        wait_for_records(&reader, hash, &[holder_address.node_id]).await;
        let holder_endpoint = holder.0.peers.as_ref().unwrap().network_endpoint().unwrap();
        let reader_endpoint = reader.0.peers.as_ref().unwrap().network_endpoint().unwrap();
        let protocol = b"test/xlb/congestion-controller";
        holder_endpoint.accept(protocol, CheckPeerController).await.unwrap();
        let holder_id = p2panda_core::VerifyingKey::from_bytes(holder_address.node_id.as_bytes()).unwrap();
        let connection = reader_endpoint.connect(holder_id, protocol).await.unwrap();
        assert!(crate::peer::await_direct_path(&connection, Duration::from_secs(5)).await);
        assert!(selected_path_uses_bbr(&connection), "dialing side must select BBRv3");
        let (mut send, mut receive) = connection.open_bi().await.unwrap();
        send.write_all(&[0]).await.unwrap();
        send.finish().unwrap();
        assert_eq!(receive.read_to_end(1).await.unwrap(), [1], "accepting side must select BBRv3");
        connection.close(iroh::endpoint::VarInt::from_u32(0), b"test complete");
        tokio::try_join!(reader.shutdown(), holder.shutdown()).unwrap();
    }).await.expect("verify the live QUIC controller on both ends");
}

async fn verify_paced_peer_fetch(stall_after_prefix: bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        let directory = tempfile::tempdir().unwrap();
        let body = test_data(96 * 1024);
        let digest = BlakeHash::hash(&body);
        let origin = OriginServer::start(digest, body.clone()).await;
        let locality = "aws/us-west-2a";
        let reader = peer_node(directory.path(), locality, PeerReach::Local, Some(origin.config(2))).await;
        let book = p2panda_net::AddressBook::builder().spawn().await.unwrap();
        let holder = fixture_endpoint(book).await;
        let sent_prefix = Arc::new(tokio::sync::Notify::new());
        holder.accept(crate::panda_chunks::CHUNK_PROTOCOL, PacedProvider {
            body: Bytes::copy_from_slice(&body), sent_prefix: sent_prefix.clone(), stall_after_prefix,
        }).await.unwrap();
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
        sent_prefix.notified().await;
        assert_eq!(origin.gets(), 0, "S3 must not race the peer");
        let (result, report) = downloading.await.unwrap();
        assert!(started.elapsed() >= crate::panda_chunks::PEER_READ_IDLE);
        assert_eq!(result.unwrap().as_ref(), body);
        let size = body.len() as u64;
        if stall_after_prefix {
            assert_eq!((report.peer_bytes, report.origin_bytes), (0, size));
            assert_eq!(origin.gets(), 1);
        } else {
            assert_eq!((report.peer_bytes, report.origin_bytes), (size, 0));
            assert_eq!(origin.gets(), 0);
        }
        assert!(peers.candidates(digest).contains(&address.id), "one timeout must not exclude the holder");
        reader.shutdown().await.unwrap();
        endpoint.close().await;
    }).await.expect("the peer fetch must complete or fall back to S3");
}

// [itest -> dsn~xlb-chunk-discovery~1,dsn~xlb-supervised-lifecycle~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_peer_times_out_before_s3_fetches_the_chunk() {
    verify_paced_peer_fetch(true).await;
}

// [itest -> dsn~xlb-chunk-discovery~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_progressing_peer_can_finish_after_the_idle_interval() {
    verify_paced_peer_fetch(false).await;
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

async fn retry_origin_after(first_get: FirstGet) -> OriginServer {
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
    origin
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test]
async fn a_throttled_origin_get_is_retried() {
    assert_eq!(retry_origin_after(FirstGet::Throttle).await.get_offsets(), [0, 0]);
}

// [itest -> dsn~xlb-swarm-fetch~1]
#[tokio::test]
async fn origin_chunk_gets_reuse_an_http1_connection() {
    let directory = tempfile::tempdir().unwrap();
    let chunks = [Bytes::from_static(b"first chunk"), Bytes::from_static(b"second chunk")]
        .map(|body| (BlakeHash::hash(&body), body));
    let origin = OriginServer::start_many(chunks.to_vec()).await;
    let mut config = AssetClassConfig::new("chunks", directory.path());
    config.origin = Some(origin.config(1));
    let node = AssetClass::register(config).await.unwrap();
    for (hash, body) in chunks {
        assert_eq!(node.indexed_chunk(hash, body.len() as u64).read_chunk().await.unwrap(), body);
    }
    assert_eq!(origin.gets(), 2);
    assert_eq!(origin.connections(), 1);
    node.shutdown().await.unwrap();
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test]
async fn an_interrupted_origin_body_resumes_from_the_received_offset() {
    const RECEIVED: usize = 64 * 1024;
    assert_eq!(retry_origin_after(FirstGet::Disconnect(RECEIVED)).await.get_offsets(), [0, RECEIVED]);
}

// [utest -> dsn~xlb-swarm-fetch~1]
#[tokio::test]
async fn fetch_records_count_a_shared_transfer_once_and_cache_reads_by_served_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let body = Bytes::from_static(b"one chunk that several callers read");
    let hash = BlakeHash::hash(&body);
    let origin = OriginServer::start(hash, body.to_vec()).await;
    let records = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut config = AssetClassConfig::new("chunks", directory.path());
    config.origin = Some(origin.config(1));
    let observed = records.clone();
    config.observer = Some(std::sync::Arc::new(move |report: &FetchReport| observed.lock().unwrap().push(report.clone())));
    let node = AssetClass::register(config).await.unwrap();
    let asset = node.indexed_chunk(hash, body.len() as u64);

    let (first, second, whole) = tokio::join!(
        asset.read_chunk_serving(TransferPriority::Demand, 4),
        asset.read_chunk_serving(TransferPriority::Demand, 4),
        asset.read_chunk(),
    );
    assert!(first.0.is_ok() && second.0.is_ok() && whole.is_ok());
    assert_eq!(asset.read_chunk_serving(TransferPriority::Demand, 5).await.0.unwrap(), body);
    let length = body.len() as u64;
    let records: Vec<_> = records.lock().unwrap().iter().map(|report| (report.tier, report.bytes_served, report.origin_bytes)).collect();
    assert_eq!(records, [(Some(FetchTier::Origin), length, length), (Some(FetchTier::Cache), 5, 0)],
        "the waiting reads share one transfer record, and the cache read records the bytes it serves");
    assert_eq!(origin.gets(), 1);
    node.shutdown().await.unwrap();
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

// Explicitly run as a benchmark; excluded from the normal test gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn benchmark_hundred_concurrent_chunk_downloads() {
    benchmark_chunk_sources(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn benchmark_hundred_concurrent_multi_peer_downloads() {
    benchmark_chunk_sources(3).await;
}

async fn benchmark_chunk_sources(holder_count: usize) {
    use futures::StreamExt;
    use std::num::NonZeroUsize;

    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).try_init();

    const CHUNKS: usize = 120;
    let directory = tempfile::tempdir().unwrap();
    let seed = test_data(IMAGE_CHUNK_BYTES);
    let chunks: Vec<_> = (0..CHUNKS).map(|ordinal| {
        let mut body = seed.clone();
        body[..8].copy_from_slice(&(ordinal as u64).to_le_bytes());
        let body = Bytes::from(body);
        (BlakeHash::hash(&body), body)
    }).collect();
    let limit = NonZeroUsize::new(100).unwrap();
    let limits = || RequestLimits::new(limit, limit, limit).unwrap();
    let s3 = OriginServer::start_many(chunks.clone()).await;
    let mut config = AssetClassConfig::new("chunks", directory.path().join("s3-client"));
    config.cas_memory_bytes = 1536 << 20;
    config.origin = Some(s3.config(100));
    let s3_limits = limits();
    config.request_limits = Some(s3_limits.clone());
    config.chunk_limit = Some(crate::ChunkAdmission::new(100));
    let client = AssetClass::register(config).await.unwrap();
    let started = Instant::now();
    let results: Vec<_> = futures::stream::iter(chunks.iter().map(|(hash, body)| {
        let client = &client;
        async move { client.indexed_chunk(*hash, body.len() as u64).read_chunk_reported().await }
    })).buffer_unordered(100).collect().await;
    let elapsed = started.elapsed();
    let success = results.iter().filter(|(result, report)| result.is_ok() && report.origin_bytes == IMAGE_CHUNK_BYTES as u64).count();
    let stats = client.s3_request_stats().unwrap();
    println!("S3: {success}/{CHUNKS} chunks, {elapsed:?}, {:.2} Gbit/s, peak {} S3 requests, {} GETs, limits {:?}", (success * IMAGE_CHUNK_BYTES) as f64 * 8.0 / elapsed.as_secs_f64() / 1e9, stats.peak_chunks, s3.gets(), s3_limits.stats());
    assert_eq!(success, CHUNKS);
    client.shutdown().await.unwrap();
    drop(client);
    drop(s3);

    let peer_limits = limits();
    let make_config = |path: &str| {
        let mut config = AssetClassConfig::new("chunks", directory.path().join(path));
        config.cas_memory_bytes = if path.starts_with("holder") { 2560 << 20 } else { 1536 << 20 };
        config.request_limits = Some(peer_limits.clone());
        config.chunk_limit = Some(crate::ChunkAdmission::new(100));
        config.peer = Some(PeerConfig { bind_addr: BIND, relay: RelaySetting::Disabled, concurrency: limit, ..PeerConfig::default() });
        config
    };
    let mut holders = Vec::new();
    for index in 0..holder_count {
        holders.push(AssetClass::register(make_config(&format!("holder-{index}"))).await.unwrap());
    }
    let client = AssetClass::register(make_config("peer-client")).await.unwrap();
    let topology = NodeTopology { provider: "aws".into(), location: "aws/us-west-2a".into(), region: None, zone: None, reach: PeerReach::Local };
    client.set_node_topology(Some(topology.clone()));
    let client_address = cache_address(&client).await;
    let mut addresses = Vec::new();
    let mut holder_ids = Vec::new();
    for holder in &holders {
        holder.set_node_topology(Some(topology.clone()));
        holder.set_peers(vec![client_address.clone()]);
        addresses.push(cache_address(holder).await);
        holder_ids.push(holder.node_id().unwrap());
        for (hash, body) in &chunks { cache_data(holder, *hash, body.clone()).await; }
        for (hash, body) in &chunks {
            assert!(holder.0.cache.get(*hash).await.unwrap().is_some(), "holder must retain chunk {hash} ({} bytes)", body.len());
        }
    }
    client.set_peers(addresses);
    for (hash, _) in &chunks { wait_for_records(&client, *hash, &holder_ids).await; }
    const ROUNDS: usize = 5;
    let started = Instant::now();
    let mut success = 0;
    let mut peer_transfers = 0;
    let mut cache_hits = 0;
    let mut peer_bytes = std::collections::BTreeMap::<String, u64>::new();
    for round in 0..ROUNDS {
        if round > 0 {
            for (hash, _) in &chunks { client.0.cache.evict_for_test(*hash); }
        }
        let results: Vec<_> = futures::stream::iter(chunks.iter().map(|(hash, body)| {
            let client = &client;
            async move { client.indexed_chunk(*hash, body.len() as u64).read_chunk_reported().await }
        })).buffer_unordered(100).collect().await;
        let completed = results.iter().filter(|(result, _)| result.is_ok()).count();
        let from_peers = results.iter().filter(|(result, report)| result.is_ok() && report.peer_bytes == IMAGE_CHUNK_BYTES as u64).count();
        let from_cache = results.iter().filter(|(result, report)| result.is_ok() && matches!(report.tier, Some(FetchTier::Cache))).count();
        peer_transfers += from_peers;
        cache_hits += from_cache;
        for (_, report) in &results {
            for (peer, bytes) in &report.peers { *peer_bytes.entry(peer.clone()).or_default() += bytes; }
        }
        success += completed;
        println!("Peer round {}: {completed}/{CHUNKS} chunks, {from_peers} peer, {from_cache} cached", round + 1);
    }
    let elapsed = started.elapsed();
    println!("Peer total: {success}/{} chunks, {peer_transfers} peer, {cache_hits} cached, {elapsed:?}, {:.2} Gbit/s, limits {:?}, peer bytes {peer_bytes:?}", CHUNKS * ROUNDS, (peer_transfers * IMAGE_CHUNK_BYTES) as f64 * 8.0 / elapsed.as_secs_f64() / 1e9, peer_limits.stats());
    client.shutdown().await.unwrap();
    for holder in holders { holder.shutdown().await.unwrap(); }
    assert_eq!(success, CHUNKS * ROUNDS);
}

/// A node fetching a model working set several times its CAS memory from S3
/// at production chunk concurrency. Resident memory after each round
/// settles must stay within the configured CAS memory.
// [itest -> dsn~xlb-foyer-cas~1]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "run as the separate origin_cas_memory_budget_test Bazel action"]
async fn origin_fetches_stay_within_the_cas_memory_budget() {
    use crate::cas_cache::resident_memory;
    use std::num::NonZeroUsize;

    const MEMORY: usize = 256 << 20;
    const BUDGET_OVERHEAD: u64 = 64 << 20;
    const CHUNKS: usize = 128;
    const ROUNDS: usize = 4;
    const CONCURRENCY: usize = 100;
    crate::allocator::map_chunk_buffers().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut chunks = Vec::with_capacity(CHUNKS);
    let mut sizes = Vec::with_capacity(CHUNKS);
    {
        let seed = test_data(IMAGE_CHUNK_BYTES);
        for ordinal in 0..CHUNKS {
            let mut body = seed.clone();
            body[..8].copy_from_slice(&(ordinal as u64).to_le_bytes());
            let body = Bytes::from(body);
            sizes.push((BlakeHash::hash(&body), body.len() as u64));
            chunks.push((BlakeHash::hash(&body), body));
        }
    }
    let s3 = OriginServer::start_many(chunks).await;
    let limit = NonZeroUsize::new(CONCURRENCY).unwrap();
    let mut config = AssetClassConfig::new("chunks", directory.path().join("node"));
    config.cas_memory_bytes = MEMORY;
    config.origin = Some(s3.config(CONCURRENCY));
    config.request_limits = Some(RequestLimits::new(limit, limit, limit).unwrap());
    config.chunk_limit = Some(crate::ChunkAdmission::new(CONCURRENCY));
    let node = AssetClass::register(config).await.unwrap();
    let baseline = resident_memory::anonymous_bytes();
    for round in 1..=ROUNDS {
        let mut fetches = tokio::task::JoinSet::new();
        for &(hash, size) in &sizes {
            let node = node.clone();
            fetches.spawn(async move { node.indexed_chunk(hash, size).read_chunk().await.map(drop) });
        }
        while let Some(fetch) = fetches.join_next().await {
            fetch.unwrap().unwrap();
        }
        node.0.cache.wait_for_storage().await;
        println!(
            "round {round}: resident growth {} MiB, Foyer memory {} MiB",
            resident_memory::anonymous_bytes().saturating_sub(baseline) >> 20,
            node.0.cache.occupancy().1 >> 20,
        );
    }
    let usage = node.0.cache.occupancy().1;
    let settled = resident_memory::anonymous_bytes().saturating_sub(baseline);
    let trimmed = resident_memory::growth_after_trim(baseline);
    println!("settled growth {} MiB, after malloc_trim {} MiB, Foyer memory {} MiB of {} MiB, {} S3 GETs",
        settled >> 20, trimmed >> 20, usage >> 20, MEMORY >> 20, s3.gets());
    node.shutdown().await.unwrap();
    assert!(usage <= MEMORY as u64, "Foyer weighs {usage} bytes against a {MEMORY} byte budget");
    assert!(settled <= MEMORY as u64 + BUDGET_OVERHEAD,
        "resident memory grew {} MiB for a {} MiB CAS budget", settled >> 20, MEMORY >> 20);
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
