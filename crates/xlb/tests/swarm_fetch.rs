//! Verify simultaneous CAS loads from gossip-discovered peers and signed S3.

mod origin_server;

use std::{net::SocketAddr, path::Path, time::Duration};

use origin_server::{OriginServer, test_data};
use xlb::{
    AssetClass, AssetClassConfig, BlakeHash, NodeTopology, PeerAddr, PeerConfig,
    RelaySetting,
};
use xlb_core::PeerReach;

const RANGE_BYTES: u64 = 128 * 1024;

async fn node(name: &'static str, dir: &Path, origin: &OriginServer) -> AssetClass {
    let mut config = AssetClassConfig::new(name, dir.join(name));
    config.peer = Some(PeerConfig {
        bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        relay: RelaySetting::Disabled,
        ..PeerConfig::default()
    });
    config.origin = Some(origin.config(RANGE_BYTES, 4));
    let class = AssetClass::register(config).await.expect("register class");
    class.set_node_topology(Some(NodeTopology {
        provider: "aws".into(),
        location: "aws/us-west-2a".into(),
        region: Some("us-west-2".into()),
        zone: Some("us-west-2a".into()),
        reach: PeerReach::Local,
    }));
    class
}

fn address(class: &AssetClass) -> PeerAddr {
    PeerAddr { node_id: class.node_id().unwrap(), addrs: class.bound_addrs(), relay_url: None }
}

// [utest -> dsn~xlb-swarm-fetch~1]
// [utest -> dsn~xlb-chunk-discovery~1]
#[tokio::test(flavor = "multi_thread")]
async fn a_new_node_reads_a_completed_peer_chunk_while_another_chunk_loads_from_s3() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let dir = tempfile::tempdir().unwrap();
        let first = test_data(512 * 1024);
        let second = test_data(480 * 1024);
        let first_hash = BlakeHash::hash(&first);
        let second_hash = BlakeHash::hash(&second);
        let peer_origin = OriginServer::start(first_hash, first.clone()).await;
        let s3 = OriginServer::start(second_hash, second.clone()).await;
        s3.pause_object_ranges(true);

        let peer = node("peer", dir.path(), &peer_origin).await;
        peer.asset(first_hash).fetch_to_file().await.expect("publish verified chunk before the join");
        let receiver = node("receiver", dir.path(), &s3).await;
        peer.set_peers(vec![address(&receiver)]);
        receiver.set_peers(vec![address(&peer)]);

        let second_asset = receiver.asset(second_hash);
        let s3_fetch = tokio::spawn(async move { second_asset.fetch_to_file_reported(None).await });
        while s3.object_ranges().iter().all(|&(_, end)| end == second.len() as u64) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let (result, report) = receiver.asset(first_hash).fetch_to_file_reported(None).await;
        assert_eq!(std::fs::read(result.expect("get the announced chunk")).unwrap(), first);
        assert_eq!(report.peer_bytes, first.len() as u64);
        assert_eq!(report.origin_bytes, 0);
        assert_eq!(report.peers.get(&peer.node_id().unwrap().to_string()), Some(&(first.len() as u64)));
        assert!(!s3_fetch.is_finished(), "the independent S3 range reads are still pending");

        s3.pause_object_ranges(false);
        let (result, report) = s3_fetch.await.unwrap();
        assert_eq!(std::fs::read(result.expect("complete S3 chunk")).unwrap(), second);
        assert_eq!(report.peer_bytes, 0);
        assert_eq!(report.origin_bytes, second.len() as u64);
        assert!(report.peers.is_empty());
        let ranges = s3.object_ranges();
        assert!(ranges.windows(2).all(|ranges| ranges[0].1 == ranges[1].0));
        assert_eq!(ranges.first().unwrap().0, 0);
        assert_eq!(ranges.last().unwrap().1, second.len() as u64);
        peer.shutdown().await;
        receiver.shutdown().await;
    }).await.expect("peer and S3 both serve the joining node");
}
