//! Own the networking subtree and project synchronized inventory into
//! the local download path. Local serving does not wait on this actor.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use futures::StreamExt;
use p2panda_core::{Body, Header, Operation, SigningKey, Timestamp, Topic, cbor, validate_operation};
use p2panda_net::sync::SyncHandle;
use p2panda_store::{SqliteStore, SqliteStoreBuilder, Transaction, logs::LogStore, operations::OperationStore, topics::TopicStore};
use p2panda_sync::protocols::TopicLogSyncEvent;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use rand::seq::SliceRandom;
use tokio::time::Instant;
use tokio_util::task::AbortOnDropHandle;
use xlb_core::peer::PeerReach;

use crate::{
    BlakeHash, Error, NodeId, NodeTopology, PeerAddr, PeerConfig, RequestLimits, Result,
    cas_cache::{CasCache, VerifiedChunk},
    inventory_publication::{InventoryPublication, Output as PublicationAction},
    panda_chunks::{ATTEMPT_TIMEOUT, CHUNK_PROTOCOL, ChunkConnections, ChunkProvider},
    panda_inventory::{InventoryRecord, InventoryView},
    panda_network::{PandaNetwork, inventory_network_id, network_error},
    peer::FailedPeers,
};

rust_fsm::state_machine! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    peer_lifecycle(Offline)

    Offline => {
        Refresh => Connecting [Start],
        TransportClosed => Offline,
        ChunkAdmitted => Offline,
    },
    Connecting => {
        Started => Online [Reconcile],
        StartupFailed => Offline [ReportFailure],
    },
    Online => {
        Refresh => Online [Reconcile],
        ChunkAdmitted => Online [Publish],
        TransportClosed => Offline [Reset],
    },
}

#[derive(Clone, Default, PartialEq, Eq)]
struct PeerSettings {
    addresses: BTreeMap<NodeId, PeerAddr>,
    admitted: BTreeSet<NodeId>,
    topology: Option<NodeTopology>,
}

#[derive(Default)]
struct PeerView {
    settings: PeerSettings,
    inventory: InventoryView,
    endpoint: Option<(p2panda_net::Endpoint, iroh::Endpoint)>,
}

type InventoryHandle = SyncHandle<Operation, TopicLogSyncEvent>;

struct TopicLog {
    handle: InventoryHandle,
    _subscription: AbortOnDropHandle<()>,
    holdings: HashSet<BlakeHash>,
    publication: InventoryPublication,
}

#[derive(Clone, Copy)]
enum InventoryUpdate<'a> {
    Observed(&'a HashSet<BlakeHash>),
    Admitted(BlakeHash),
}

enum PeerMessage {
    Refresh,
    ChunkAdmitted(BlakeHash),
    Received(Topic, Box<Operation>),
    TransportClosed,
}

struct PeerRuntime {
    lifecycle: peer_lifecycle::StateMachine,
    store: Option<SqliteStore>,
    topics: HashMap<Topic, TopicLog>,
    network: Option<PandaNetwork>,
    bootstrap: BTreeMap<NodeId, PeerAddr>,
    _heartbeat: AbortOnDropHandle<()>,
    endpoint_watch: Option<AbortOnDropHandle<()>>,
}

struct PeerWorker {
    cache: Arc<CasCache>,
    view: Arc<RwLock<PeerView>>,
    identity: SigningKey,
    config: PeerConfig,
    network_id: [u8; 32],
}

impl Actor for PeerWorker {
    type Msg = PeerMessage;
    type State = PeerRuntime;
    type Arguments = ();

    async fn pre_start(&self, myself: ActorRef<Self::Msg>, _: ()) -> Result<Self::State, ActorProcessingErr> {
        myself.cast(PeerMessage::Refresh)?;
        Ok(PeerRuntime {
            lifecycle: peer_lifecycle::StateMachine::new(),
            store: None,
            topics: HashMap::new(),
            network: None,
            bootstrap: BTreeMap::new(),
            _heartbeat: AbortOnDropHandle::new(myself.send_interval(Duration::from_secs(1), || PeerMessage::Refresh)),
            endpoint_watch: None,
        })
    }

    async fn handle(
        &self, myself: ActorRef<Self::Msg>, message: Self::Msg, state: &mut PeerRuntime,
    ) -> Result<(), ActorProcessingErr> {
        let result = match message {
            PeerMessage::Refresh => self.drive_lifecycle(myself, state, peer_lifecycle::Input::Refresh, None).await,
            PeerMessage::ChunkAdmitted(hash) => self.drive_lifecycle(myself, state, peer_lifecycle::Input::ChunkAdmitted, Some(hash)).await,
            PeerMessage::Received(topic, operation) => self.ingest(state, topic, &operation).await,
            PeerMessage::TransportClosed => self.drive_lifecycle(myself, state, peer_lifecycle::Input::TransportClosed, None).await,
        };
        if let Err(error) = result {
            tracing::warn!(%error, "XLB peer inventory operation failed");
        }
        Ok(())
    }

    async fn post_stop(&self, _: ActorRef<Self::Msg>, state: &mut PeerRuntime) -> Result<(), ActorProcessingErr> {
        let endpoint = self.view.write().unwrap().endpoint.take();
        if let Some((_, endpoint)) = endpoint {
            endpoint.close().await;
        }
        self.clear_network(state);
        Ok(())
    }
}

impl PeerWorker {
    async fn drive_lifecycle(
        &self, myself: ActorRef<PeerMessage>, state: &mut PeerRuntime, mut event: peer_lifecycle::Input,
        admitted: Option<BlakeHash>,
    ) -> Result<()> {
        use peer_lifecycle::{Input, Output};

        let mut startup = Ok(());
        loop {
            event = match state.lifecycle.consume(&event).expect("valid peer lifecycle transition") {
                Some(Output::Start) => {
                    startup = self.start_network(&myself, state).await;
                    if startup.is_ok() { Input::Started } else { Input::StartupFailed }
                }
                Some(Output::Reconcile) => return self.refresh(myself, state).await,
                Some(Output::Publish) => return self.publish(state, admitted).await,
                Some(Output::ReportFailure) => return startup,
                Some(Output::Reset) => {
                    self.clear_network(state);
                    return Ok(());
                }
                None => return Ok(()),
            };
        }
    }

    fn clear_network(&self, state: &mut PeerRuntime) {
        self.view.write().unwrap().endpoint = None;
        state.endpoint_watch = None;
        state.topics.clear();
        state.bootstrap.clear();
        state.network = None;
    }

    async fn start_network(&self, myself: &ActorRef<PeerMessage>, state: &mut PeerRuntime) -> Result<()> {
        if state.store.is_none() {
            state.store = Some(SqliteStoreBuilder::memory().build().await.map_err(network_error)?);
        }
        let store = state.store.as_ref().expect("peer inventory store").clone();
        let network = PandaNetwork::start(&self.config, self.network_id, self.identity.clone(), store).await?;
        let view = Arc::downgrade(&self.view);
        network.endpoint.accept(CHUNK_PROTOCOL, ChunkProvider {
            cache: self.cache.clone(),
            admitted: Arc::new(move |node| view.upgrade().is_some_and(|view| {
                view.read().unwrap().settings.admitted.contains(&node)
            })),
            concurrency: self.config.concurrency.get(),
        }).await.map_err(network_error)?;
        let raw_endpoint = network.endpoint.endpoint().await.map_err(network_error)?;
        self.view.write().unwrap().endpoint = Some((network.endpoint.clone(), raw_endpoint.clone()));
        state.network = Some(network);
        let actor = myself.clone();
        state.endpoint_watch = Some(AbortOnDropHandle::new(tokio::spawn(async move {
            raw_endpoint.closed().await;
            let _ = actor.cast(PeerMessage::TransportClosed);
        })));
        Ok(())
    }

    // [impl -> dsn~xlb-chunk-discovery~1]
    async fn refresh(&self, myself: ActorRef<PeerMessage>, state: &mut PeerRuntime) -> Result<()> {
        let network = state.network.as_ref().expect("the network is ready");
        let settings = self.view.read().unwrap().settings.clone();
        for (node, address) in &settings.addresses {
            if state.bootstrap.get(node) != Some(address) {
                network.add_bootstrap(address).await?;
                state.bootstrap.insert(*node, address.clone());
            }
        }
        state.bootstrap.retain(|node, _| settings.addresses.contains_key(node));
        let mut topics = HashSet::new();
        if let Some(topology) = &settings.topology {
            topics.insert(inventory_topic(None));
            topics.insert(inventory_topic(Some(&topology.location)));
        }
        let removed: Vec<_> = state.topics.keys().filter(|topic| !topics.contains(topic)).copied().collect();
        for topic in removed {
            if let Some(log) = state.topics.remove(&topic) {
                log.handle.close().await.map_err(network_error)?;
            }
        }
        for topic in topics {
            if state.topics.contains_key(&topic) {
                continue;
            }
            let handle = network.sync.stream(topic, true).await.map_err(network_error)?;
            let mut events = handle.subscribe().await.map_err(network_error)?;
            let actor = myself.clone();
            let subscription = tokio::spawn(async move {
                while let Some(event) = events.next().await {
                    let Ok(event) = event else { continue };
                    let TopicLogSyncEvent::OperationReceived { operation, .. } = event.event else {
                        continue;
                    };
                    if actor.cast(PeerMessage::Received(topic, operation)).is_err() {
                        break;
                    }
                }
            });
            state.topics.insert(topic, TopicLog {
                handle, _subscription: AbortOnDropHandle::new(subscription), holdings: HashSet::new(),
                publication: InventoryPublication::new(),
            });
        }
        self.publish(state, None).await
    }

    async fn publish(&self, state: &mut PeerRuntime, admitted: Option<BlakeHash>) -> Result<()> {
        let Some(network) = &state.network else { return Ok(()) };
        let (topology, address) = {
            let view = self.view.read().unwrap();
            let (Some(topology), Some((_, endpoint))) = (&view.settings.topology, &view.endpoint) else { return Ok(()) };
            (topology.clone(), endpoint.addr())
        };
        let holdings;
        let update = match admitted {
            Some(hash) if self.cache.may_hold(hash) => InventoryUpdate::Admitted(hash),
            Some(_) => return Ok(()),
            None => {
                holdings = self.cache.complete_hashes().into_iter().collect();
                InventoryUpdate::Observed(&holdings)
            }
        };
        for (topic, log) in &mut state.topics {
            if *topic == inventory_topic(None) && topology.reach != PeerReach::Global {
                continue;
            }
            let now = std::time::Instant::now();
            let action = match update {
                InventoryUpdate::Observed(current) => {
                    log.publication.reconcile(now, *current != log.holdings, self.config.inventory_expiry)
                }
                InventoryUpdate::Admitted(hash) if !log.holdings.contains(&hash) => log.publication.admit(),
                InventoryUpdate::Admitted(_) => None,
            };
            let Some(action) = action else { continue };
            let (added, removed) = match (action, update) {
                (PublicationAction::Checkpoint, InventoryUpdate::Observed(current)) => {
                    (current.iter().copied().collect(), Vec::new())
                }
                (PublicationAction::Delta, InventoryUpdate::Observed(current)) => (
                    current.difference(&log.holdings).copied().collect(),
                    log.holdings.difference(current).copied().collect(),
                ),
                (PublicationAction::Delta, InventoryUpdate::Admitted(hash)) => (vec![hash], Vec::new()),
                (PublicationAction::Checkpoint, InventoryUpdate::Admitted(_)) => {
                    unreachable!("a checkpoint requires a complete inventory")
                }
            };
            let record = InventoryRecord {
                address: address.clone(), topology: topology.clone(),
                checkpoint: action == PublicationAction::Checkpoint,
                issued_at: Timestamp::now(), added, removed,
            };
            let result = append_record(network, *topic, &record).await
                .and_then(|operation| log.handle.publish(operation).map_err(network_error));
            log.publication.finish(now, result.is_ok());
            result?;
            match update {
                InventoryUpdate::Observed(current) => log.holdings.clone_from(current),
                InventoryUpdate::Admitted(hash) => {
                    log.holdings.insert(hash);
                }
            }
        }
        Ok(())
    }

    async fn ingest(&self, state: &PeerRuntime, topic: Topic, operation: &Operation) -> Result<()> {
        let Some(network) = &state.network else { return Ok(()) };
        if !state.topics.contains_key(&topic) || validate_operation(operation).is_err() {
            return Ok(());
        }
        let Some(body) = &operation.body else { return Ok(()) };
        let record: InventoryRecord = cbor::decode_cbor_strict(body.as_bytes()).map_err(network_error)?;
        if record.address.id.as_bytes() != operation.header.verifying_key.as_bytes()
            || (topic == inventory_topic(None) && record.topology.reach != PeerReach::Global)
            || (topic != inventory_topic(None) && topic != inventory_topic(Some(&record.topology.location)))
        {
            return Ok(());
        }
        let authorized = self.view.read().unwrap().settings.addresses.contains_key(&record.address.id);
        if !authorized {
            return Ok(());
        }
        let transaction = network.store.begin().await.map_err(network_error)?;
        save_transaction(network, topic, operation, record.checkpoint).await?;
        network.store.commit(transaction).await.map_err(network_error)?;
        self.view.write().unwrap().inventory.apply(
            topic, operation.header.seq_num, record,
        );
        Ok(())
    }
}

async fn append_record(network: &PandaNetwork, topic: Topic, record: &InventoryRecord) -> Result<Operation> {
    let body = Body::from_bytes(cbor::encode_cbor(record).map_err(network_error)?);
    let author = network.identity.verifying_key();
    let transaction = network.store.begin().await.map_err(network_error)?;
    let (sequence, backlink) = network.store.get_latest_entry_tx(&author, &log_id(topic)).await.map_err(network_error)?
        .map(|previous| (previous.header.seq_num + 1, Some(previous.hash))).unwrap_or((0, None));
    let header = Header::builder().seq_num(sequence).backlink(backlink).body(&body).build(&network.identity, ());
    let operation = Operation::from_parts(header, Some(body));
    save_transaction(network, topic, &operation, record.checkpoint).await?;
    network.store.commit(transaction).await.map_err(network_error)?;
    Ok(operation)
}

async fn save_transaction(network: &PandaNetwork, topic: Topic, operation: &Operation, checkpoint: bool) -> Result<()> {
    let author = operation.header.verifying_key;
    let id = log_id(topic);
    network.store.insert_operation_tx(&operation.hash, operation, &id).await.map_err(network_error)?;
    network.store.associate_tx(&topic, &author, &id).await.map_err(network_error)?;
    if checkpoint && operation.header.seq_num > 0 {
        network.store.prune_entries_tx(&author, &id, &operation.header.seq_num).await.map_err(network_error)?;
    }
    Ok(())
}

fn inventory_topic(location: Option<&str>) -> Topic {
    let mut hash = blake3::Hasher::new_derive_key("xlb chunk inventory topic v1");
    if let Some(location) = location {
        hash.update(location.as_bytes());
    }
    (*hash.finalize().as_bytes()).into()
}

fn log_id(topic: Topic) -> u64 {
    u64::from_le_bytes(topic.as_bytes()[..8].try_into().expect("a topic has 32 bytes"))
}

pub(crate) struct PandaPeers {
    view: Arc<RwLock<PeerView>>,
    worker: ActorRef<PeerMessage>,
    node: NodeId,
    limits: RequestLimits,
    config: PeerConfig,
    failed: FailedPeers,
    connections: ChunkConnections,
}

impl PandaPeers {
    pub(crate) async fn start(config: PeerConfig, cache: Arc<CasCache>, limits: RequestLimits, class: &str) -> Result<Self> {
        let identity = SigningKey::generate();
        let node = NodeId::from_bytes(identity.verifying_key().as_bytes()).map_err(network_error)?;
        let view = Arc::new(RwLock::new(PeerView::default()));
        let worker = PeerWorker { cache, view: view.clone(), identity, config: config.clone(), network_id: inventory_network_id(class) };
        let (worker, _) = Actor::spawn(None, worker, ()).await.map_err(network_error)?;
        Ok(Self {
            view, worker, node, limits, config, failed: FailedPeers::default(),
            connections: ChunkConnections::default(),
        })
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.node
    }

    pub(crate) fn bound_addrs(&self) -> Vec<std::net::SocketAddr> {
        self.view.read().unwrap().endpoint.as_ref().map(|(_, endpoint)| endpoint.bound_sockets()).unwrap_or_default()
    }

    pub(crate) fn relay_url(&self) -> Option<iroh::RelayUrl> {
        let view = self.view.read().unwrap();
        view.endpoint.as_ref()?.1.addr().relay_urls().next().cloned()
    }

    pub(crate) fn peers(&self) -> Vec<PeerAddr> {
        self.view.read().unwrap().settings.addresses.values().cloned().collect()
    }

    pub(crate) fn set_peers(&self, peers: Vec<PeerAddr>, admitted: Vec<NodeId>) {
        let mut view = self.view.write().unwrap();
        view.settings.addresses = peers.into_iter().filter(|peer| peer.node_id != self.node).map(|peer| (peer.node_id, peer)).collect();
        view.settings.admitted = admitted.into_iter().filter(|node| *node != self.node).collect();
        self.connections.retain(|node| view.settings.addresses.contains_key(node));
        drop(view);
        let _ = self.worker.cast(PeerMessage::Refresh);
    }

    pub(crate) fn set_topology(&self, topology: Option<NodeTopology>) {
        self.view.write().unwrap().settings.topology = topology;
        let _ = self.worker.cast(PeerMessage::Refresh);
    }

    pub(crate) fn announce(&self, hash: BlakeHash) {
        let _ = self.worker.cast(PeerMessage::ChunkAdmitted(hash));
    }

    #[cfg(test)]
    pub(super) fn project_test_inventory(&self, record: InventoryRecord) {
        let topic = inventory_topic(Some(&record.topology.location));
        self.view.write().unwrap().inventory.apply(topic, 0, record);
    }

    #[cfg(test)]
    pub(super) fn knows_holder(&self, hash: BlakeHash, location: &str, node: NodeId) -> bool {
        let view = self.view.read().unwrap();
        let (local, global) = view.inventory.candidates(hash, location, self.config.inventory_expiry, Timestamp::now());
        local.contains(&node) || global.contains(&node)
    }

    #[cfg(test)]
    pub(super) fn transport_endpoint(&self) -> Option<iroh::Endpoint> {
        self.view.read().unwrap().endpoint.as_ref().map(|(_, endpoint)| endpoint.clone())
    }

    pub(crate) fn candidates(&self, hash: BlakeHash) -> Vec<NodeId> {
        let view = self.view.read().unwrap();
        let Some(topology) = &view.settings.topology else { return Vec::new() };
        if self.config.disable_sourcing || view.endpoint.is_none() {
            return Vec::new();
        }
        let (local, mut global) = view.inventory.candidates(hash, &topology.location, self.config.inventory_expiry, Timestamp::now());
        global.retain(|node| !local.contains(node));
        let eligible = |node: &&NodeId| view.settings.addresses.contains_key(*node) && !self.failed.contains(node, Instant::now());
        let mut local: Vec<_> = local.iter().filter(eligible).copied().collect();
        let mut global: Vec<_> = global.iter().filter(eligible).copied().collect();
        local.shuffle(&mut rand::rng());
        global.shuffle(&mut rand::rng());
        local.extend(global);
        local
    }

    pub(crate) async fn fetch(&self, node: NodeId, hash: BlakeHash, size: usize) -> Result<Option<VerifiedChunk>> {
        let _slot = self.limits.peer().await;
        let endpoint = {
            let view = self.view.read().unwrap();
            if !view.settings.addresses.contains_key(&node) || self.failed.contains(&node, Instant::now()) {
                return Ok(None);
            }
            view.endpoint.as_ref().map(|(endpoint, _)| endpoint.clone())
        };
        let Some(endpoint) = endpoint else { return Ok(None) };
        let transfer = async {
            let bytes = self.connections.fetch(&endpoint, node, hash, size).await.map_err(Error::Store)?;
            VerifiedChunk::verify(hash, size as u64, bytes).await.map(Some)
        };
        let result = tokio::time::timeout(ATTEMPT_TIMEOUT, transfer).await
            .unwrap_or_else(|_| Err(Error::Store("peer attempt timed out".into())));
        if result.is_err() {
            self.failed.insert(node, Instant::now());
        }
        result
    }

    pub(crate) async fn shutdown(&self) {
        let _ = self.worker.stop_and_wait(None, None).await;
    }
}
