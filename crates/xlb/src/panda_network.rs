//! The p2panda services for one XLB node.

use std::net::SocketAddr;

use p2panda_core::SigningKey;
use p2panda_net::{
    AddressBook, Discovery, Endpoint, Gossip, LogSync, Supervisor,
    addrs::NodeInfo, iroh_endpoint::IrohConfig,
};
use p2panda_store::SqliteStore;

use crate::{Error, PeerAddr, PeerConfig, Result};

pub(crate) type InventorySync = LogSync<SqliteStore, u64, ()>;

pub(crate) fn inventory_network_id(class: &str) -> [u8; 32] {
    let mut hash = blake3::Hasher::new_derive_key("xlb p2panda network v1");
    hash.update(class.as_bytes());
    *hash.finalize().as_bytes()
}

pub(crate) struct PandaNetwork {
    pub(crate) sync: InventorySync,
    _gossip: Gossip,
    _discovery: Discovery,
    pub(crate) endpoint: Endpoint,
    pub(crate) address_book: AddressBook,
    pub(crate) store: SqliteStore,
    pub(crate) identity: SigningKey,
    _supervisor: Supervisor,
}

impl PandaNetwork {
    // [impl -> dsn~xlb-supervised-lifecycle~1]
    pub(crate) async fn start(
        config: &PeerConfig, pool: [u8; 32], identity: SigningKey, store: SqliteStore,
    ) -> Result<Self> {
        let supervisor = Supervisor::builder().spawn().await.map_err(network_error)?;
        let address_book = AddressBook::builder().spawn().await.map_err(network_error)?;
        let mut bind = IrohConfig::default();
        match config.bind_addr {
            SocketAddr::V4(address) => {
                bind.bind_ip_v4 = *address.ip();
                bind.bind_port_v4 = address.port();
            }
            SocketAddr::V6(address) => {
                bind.bind_ip_v6 = *address.ip();
                bind.bind_port_v6 = address.port();
            }
        }
        let mut endpoint = Endpoint::builder(address_book.clone())
            .network_id(pool)
            .config(bind)
            .signing_key(identity.clone());
        for relay in config.relay.relay_mode().map_err(Error::Config)?.relay_map().urls::<Vec<_>>() {
            endpoint = endpoint.relay_url(relay);
        }
        let endpoint = endpoint.spawn_linked(&supervisor).await.map_err(network_error)?;
        let discovery = Discovery::builder(address_book.clone(), endpoint.clone())
            .spawn_linked(&supervisor).await.map_err(network_error)?;
        let gossip = Gossip::builder(address_book.clone(), endpoint.clone()).spawn().await.map_err(network_error)?;
        let sync = LogSync::builder(store.clone(), endpoint.clone(), gossip.clone())
            .spawn().await.map_err(network_error)?;
        Ok(Self {
            sync,
            _gossip: gossip,
            _discovery: discovery,
            endpoint,
            address_book,
            store,
            identity,
            _supervisor: supervisor,
        })
    }

    pub(crate) async fn add_bootstrap(&self, peer: &PeerAddr) -> Result<()> {
        let relay = peer.relay_url.iter().cloned().map(iroh::TransportAddr::Relay);
        let direct = peer.addrs.iter().copied().map(iroh::TransportAddr::Ip);
        let address = iroh::EndpointAddr::from_parts(peer.node_id, relay.chain(direct));
        self.address_book.insert_node_info(NodeInfo::from(address).bootstrap()).await.map_err(network_error)?;
        Ok(())
    }

}

pub(crate) fn network_error(error: impl std::fmt::Display) -> Error {
    Error::Store(format!("p2panda: {error}"))
}
