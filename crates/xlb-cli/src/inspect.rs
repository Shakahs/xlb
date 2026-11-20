//! `xlb inspect` and `xlb set-peers`.

use anyhow::{bail, Context, Result};
use xlb_client::{Client, Command, Peer, Response};

pub async fn run(socket_path: &str) -> Result<()> {
    let info = Client::connect(socket_path).await?.inspect().await?;
    println!("node_id  : {}", info.node_id.as_deref().unwrap_or("none"));
    println!("udp      : {}", info.udp_addrs.join(", "));
    println!("relay    : {}", info.relay_url.as_deref().unwrap_or("none"));
    println!("cache   : {}", info.cache_dir.as_deref().unwrap_or("none"));
    println!("uptime   : {}s", info.uptime_secs);
    println!("classes  : {}", info.classes.len());
    for class in &info.classes {
        println!("  {}", class.name);
        println!("    node_id : {}", class.node_id.as_deref().unwrap_or("none"));
        println!("    udp     : {}", class.udp_addrs.join(", "));
        println!("    cache   : {}", class.cache_dir);
        println!("    budget  : {} bytes", class.cache_budget_bytes);
        println!("    origin  : {}", if class.origin { "yes" } else { "no" });
        println!("    peers   : {}", class.peer_count);
    }
    Ok(())
}

/// Parse `NODE_ID@ADDR[,ADDR...]`.
fn parse_peer(spec: &str) -> Result<Peer> {
    let (node_id, addrs) = spec
        .split_once('@')
        .with_context(|| format!("peer {spec:?} is not NODE_ID@ADDR[,ADDR...]"))?;
    Ok(Peer {
        node_id: node_id.to_string(),
        addrs: addrs.split(',').map(str::to_string).collect(),
        relay_url: None,    })
}

pub async fn set_peers(socket_path: &str, class: Option<&str>, peers: &[&str]) -> Result<()> {
    let command = Command::SetPeers {
        peers: peers.iter().map(|p| parse_peer(p)).collect::<Result<_>>()?,
        admit: None,
        class: class.map(str::to_string),
        topology: None,
    };
    match Client::connect(socket_path)
        .await?
        .roundtrip(&command)
        .await?
    {
        Response::PeersSet { classes } => {
            println!("{} peer(s) set on {classes} class(es)", peers.len());
            Ok(())
        }
        Response::Error { message } => bail!("{message}"),
        other => bail!("unexpected response: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_peer_is_a_node_id_with_one_or_more_addresses() {
        let peer = parse_peer("abcd@10.0.0.1:4433,10.0.0.2:4433").unwrap();
        assert_eq!(peer.node_id, "abcd");
        assert_eq!(peer.addrs, ["10.0.0.1:4433", "10.0.0.2:4433"]);
        assert!(parse_peer("abcd").is_err());
    }
}
