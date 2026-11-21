//! Signed availability records on XLB's iroh-gossip topic.

use std::{collections::BTreeSet, time::{Duration, SystemTime, UNIX_EPOCH}};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr};
use iroh_blobs::Hash;
use iroh_gossip::{Gossip, TopicId};
use iroh_smol_kv::{Client, Config, ExpiryConfig, Filter, Subscribe, SubscribeItem, SubscribeMode, WriteScope};
use serde::Serialize;
use tokio::sync::watch;
use tokio_util::task::AbortOnDropHandle;

use crate::{BlakeHash, NodeTopology};

const REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const ENTRY_TTL: Duration = Duration::from_secs(90);
const JOIN_SYNC_INTERVAL: Duration = Duration::from_secs(1);
const TOPIC_NAME: &[u8] = b"xlb/cas-provider-discovery/v1";

#[derive(Serialize)]
struct NodeRecord {
    addresses: BTreeSet<TransportAddr>,
    #[serde(flatten)]
    topology: NodeTopology,
}

pub(crate) enum ProviderEvent {
    Available { address: EndpointAddr, location: String, timestamp: u64 },
    Expired { node: EndpointId, timestamp: u64 },
    CurrentDone,
}

pub(crate) struct Discovery {
    client: Client,
    writer: WriteScope,
    endpoint: Endpoint,
    topology: watch::Sender<Option<NodeTopology>>,
    refresh: AbortOnDropHandle<()>,
}

impl Discovery {
    // [impl -> dsn~xlb-chunk-discovery~1]
    pub(crate) async fn join(
        gossip: &Gossip,
        endpoint: Endpoint,
        complete_hashes: impl Fn() -> Vec<Hash> + Send + 'static,
    ) -> Result<Self, String> {
        let topic = gossip.subscribe(discovery_topic(), Vec::new()).await.map_err(|err| err.to_string())?;
        let client = Client::local(topic, Config {
            expiry: Some(ExpiryConfig { horizon: ENTRY_TTL, check_interval: REFRESH_INTERVAL }),
            fast_anti_entropy_interval: JOIN_SYNC_INTERVAL,
            ..Config::default()
        });
        let writer = client.write(endpoint.secret_key().clone());
        let own = endpoint.clone();
        let announce = writer.clone();
        let (topology, mut metadata) = watch::channel::<Option<NodeTopology>>(None);
        let refresh = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut interval = tokio::time::interval(REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {},
                    changed = metadata.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                }
                let topology = metadata.borrow_and_update().clone();
                if let Some(topology) = topology {
                    let location = location_key(&topology.location);
                    publish_node(&announce, &own, topology).await;
                    for hash in complete_hashes() {
                        publish(&announce, &own, hash, &location).await;
                    }
                }
            }
        }));
        Ok(Self { client, writer, endpoint, topology, refresh })
    }

    pub(crate) fn bootstrap(&self, peers: Vec<EndpointId>) {
        if !peers.is_empty() {
            let client = self.client.clone();
            tokio::spawn(async move {
                if let Err(err) = client.join_peers(peers).await {
                    tracing::warn!(%err, "join XLB availability gossip");
                }
            });
        }
    }

    pub(crate) async fn announce(&self, hash: Hash) {
        let location = self.topology.borrow().as_ref().map(|topology| location_key(&topology.location));
        if let Some(location) = location {
            publish(&self.writer, &self.endpoint, hash, &location).await;
        }
    }

    pub(crate) fn shares_location(&self, location: &str) -> bool {
        self.topology.borrow().as_ref().is_some_and(|topology| location_key(&topology.location) == location)
    }

    pub(crate) fn set_node_topology(&self, topology: Option<NodeTopology>) {
        self.topology.send_if_modified(|current| {
            if *current == topology {
                return false;
            }
            *current = topology;
            true
        });
    }

    pub(crate) fn providers(&self, hash: Hash) -> impl Stream<Item = ProviderEvent> + Send + 'static {
        let filter = Filter::ALL
            .key_prefix(provider_prefix(hash))
            .timestamps((SystemTime::now() - ENTRY_TTL)..);
        self.client.subscribe_with_opts(Subscribe { mode: SubscribeMode::Both, filter })
            .stream_raw()
            .filter_map(move |item| async move {
                match item {
                    Ok(SubscribeItem::Entry((node, key, value))) => {
                        let location = provider_location(&key, hash, node)?.to_owned();
                        let addrs: BTreeSet<TransportAddr> = serde_json::from_slice(&value.value).ok()?;
                        let address = EndpointAddr { id: node, addrs };
                        Some(ProviderEvent::Available { address, location, timestamp: value.timestamp })
                    }
                    Ok(SubscribeItem::Expired((node, key, timestamp))) if provider_location(&key, hash, node).is_some() => {
                        Some(ProviderEvent::Expired { node, timestamp })
                    }
                    Ok(SubscribeItem::CurrentDone) => Some(ProviderEvent::CurrentDone),
                    Ok(_) => None,
                    Err(err) => {
                        tracing::warn!(%err, "read XLB availability gossip");
                        None
                    }
                }
            })
    }

    pub(crate) async fn shutdown(&self) {
        self.refresh.abort();
        if let Err(err) = self.client.shutdown().await {
            tracing::warn!(%err, "shut down XLB availability gossip");
        }
    }
}

pub(crate) fn is_fresh(timestamp: u64) -> bool {
    is_fresh_at(timestamp, SystemTime::now())
}

fn is_fresh_at(timestamp: u64, now: SystemTime) -> bool {
    let oldest = now.duration_since(UNIX_EPOCH).unwrap_or_default().saturating_sub(ENTRY_TTL);
    u128::from(timestamp) > oldest.as_nanos()
}

fn provider_prefix(hash: Hash) -> String {
    format!("CAS/{hash}/")
}

fn location_key(location: &str) -> String {
    location.replace('%', "%25").replace('/', "%2F")
}

fn provider_key(hash: Hash, location: &str, node: EndpointId) -> Bytes {
    let prefix = provider_prefix(hash);
    format!("{prefix}{location}/{node}").into()
}

fn provider_location(key: &[u8], hash: Hash, node: EndpointId) -> Option<&str> {
    let suffix = std::str::from_utf8(key).ok()?.strip_prefix(&provider_prefix(hash))?;
    let (location, id) = suffix.split_once('/')?;
    (!location.is_empty() && id == node.to_string()).then_some(location)
}

pub(crate) fn discovery_topic() -> TopicId {
    TopicId::from_bytes(*BlakeHash::hash(TOPIC_NAME).as_bytes())
}

async fn publish_node(writer: &WriteScope, endpoint: &Endpoint, topology: NodeTopology) {
    let addr = endpoint.addr();
    let record = NodeRecord { addresses: addr.addrs, topology };
    let value = match serde_json::to_vec(&record) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(%err, "encode XLB node record");
            return;
        }
    };
    if let Err(err) = writer.put(format!("nodes/{}", addr.id), value).await {
        tracing::warn!(%err, "announce XLB node");
    }
}

async fn publish(writer: &WriteScope, endpoint: &Endpoint, hash: Hash, location: &str) {
    let addr = endpoint.addr();
    let value = match serde_json::to_vec(&addr.addrs) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(%err, %hash, "encode XLB provider addresses");
            return;
        }
    };
    if let Err(err) = writer.put(provider_key(hash, location, addr.id), value).await {
        tracing::warn!(%err, %hash, "announce XLB provider");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use iroh::{RelayMode, address_lookup::MemoryLookup, endpoint::presets};
    use xlb_core::PeerReach;
    use crate::{AssetClass, AssetClassConfig, PeerConfig, RelaySetting};

    // [utest -> dsn~xlb-chunk-discovery~1]
    #[test]
    fn availability_uses_the_chunk_prefix_and_expires_after_ninety_seconds() {
        let hash = Hash::from_bytes([7; 32]);
        let node = iroh::SecretKey::generate().public();
        let hash_hex = hash.to_hex();
        let node_hex = hex::encode(node.as_bytes());
        assert_eq!(provider_prefix(hash), format!("CAS/{hash_hex}/"));
        let encoded = location_key("aws/us-west-2a");
        let key = provider_key(hash, &encoded, node);
        assert_eq!(key, format!("CAS/{hash_hex}/aws%2Fus-west-2a/{node_hex}"));
        assert_eq!(provider_location(&key, hash, node), Some("aws%2Fus-west-2a"));

        let timestamp = 120_000_000_000;
        let now = UNIX_EPOCH + Duration::from_nanos(timestamp);
        assert!(is_fresh_at(timestamp, now + Duration::from_secs(89)));
        assert!(!is_fresh_at(timestamp, now + Duration::from_secs(90)));
    }

    // [utest -> dsn~xlb-chunk-discovery~1]
    #[tokio::test]
    async fn a_node_replicates_its_addresses_and_configured_topology() {
        tokio::time::timeout(Duration::from_secs(15), async {
            let directory = tempfile::tempdir().unwrap();
            let bind = SocketAddr::from(([127, 0, 0, 1], 0));
            let mut config = AssetClassConfig::new("models", directory.path());
            config.peer = Some(PeerConfig { bind_addr: bind, relay: RelaySetting::Disabled, ..PeerConfig::default() });
            let node = AssetClass::register(config).await.unwrap();
            let node_id = node.node_id().unwrap();
            let address = node.bound_addrs()[0];
            let lookup = MemoryLookup::from_endpoint_info([EndpointAddr::from_parts(node_id, [TransportAddr::Ip(address)])]);
            let endpoint = Endpoint::builder(presets::Minimal)
                .relay_mode(RelayMode::Disabled)
                .address_lookup(lookup)
                .clear_ip_transports()
                .bind_addr(bind).unwrap()
                .bind().await.unwrap();
            let gossip = Gossip::builder().spawn(endpoint.clone());
            let router = iroh::protocol::Router::builder(endpoint)
                .accept(iroh_gossip::ALPN, gossip.clone()).spawn();
            let topic = gossip.subscribe_and_join(discovery_topic(), vec![node_id]).await.unwrap();
            let replica = Client::local(topic, Config::default());
            let records = replica.subscribe_with_opts(Subscribe {
                mode: SubscribeMode::Both,
                filter: Filter::ALL.key_prefix("nodes/"),
            }).stream_raw();
            tokio::pin!(records);

            node.set_node_topology(Some(NodeTopology {
                provider: "aws".into(),
                location: "aws/us-west-2a".into(),
                region: Some("us-west-2".into()),
                zone: Some("us-west-2a".into()),
                reach: PeerReach::Local,
            }));
            let (scope, key, value) = loop {
                if let SubscribeItem::Entry(record) = records.next().await.unwrap().unwrap() {
                    break record;
                }
            };
            assert_eq!(scope, node_id);
            assert_eq!(key, format!("nodes/{node_id}"));
            let record: serde_json::Value = serde_json::from_slice(&value.value).unwrap();
            assert_eq!(record["provider"], "aws");
            assert_eq!(record["region"], "us-west-2");
            assert_eq!(record["zone"], "us-west-2a");
            assert_eq!(record["reach"], "local");
            let addresses: BTreeSet<TransportAddr> = serde_json::from_value(record["addresses"].clone()).unwrap();
            assert!(addresses.contains(&TransportAddr::Ip(address)), "{addresses:?}");

            replica.shutdown().await.unwrap();
            router.shutdown().await.unwrap();
            node.shutdown().await;
        }).await.expect("the node record reaches the gossip replica");
    }
}
