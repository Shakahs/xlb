//! `xlb serve` — register the configured classes and answer the socket.

use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result};
use tokio::sync::broadcast;
use xlb::{AssetClass, AssetClassConfig};

use crate::{
    config::NodeConfig,
    metrics_sink::MetricsSink,
    server::{ClassEntry, NodeState},
};

pub async fn run(config_path: &str, socket_flag: Option<&str>) -> Result<()> {
    let cfg = NodeConfig::load(config_path)?;
    let socket = effective_socket(socket_flag, cfg.node.socket.as_deref());
    let socket_mode = cfg.node.socket_mode()?;

    // Built before the classes because every class's observer is a handle to
    // it; the node ID is filled in once a class has bound its endpoint.
    let (event_tx, _) = broadcast::channel(256);
    let metrics = Arc::new(MetricsSink::new(
        String::new(),
        cfg.node.peer_class.clone(),
        event_tx.clone(),
        cfg.node.metrics_path.as_deref(),
    )?);

    let mut classes = Vec::new();
    for cc in &cfg.classes {
        // AssetClassConfig.name is &'static str; leak each name once at startup.
        let name: &'static str = Box::leak(cc.name.clone().into_boxed_str());
        let mut config = AssetClassConfig::new(name, cc.cache_dir());
        config.cache_budget_bytes = cc.cache_budget_bytes;
        config.peer = cc.peer_config()?;
        config.origin = cc.origin_config()?;
        config.observer = Some(metrics.observer());
        let has_origin = config.origin.is_some();
        let class = AssetClass::register(config)
            .await
            .with_context(|| format!("register class {}", cc.name))?;
        classes.push(ClassEntry {
            name: cc.name.clone(),
            class,
            has_origin,
        });
    }

    let state = NodeState {
        started_at: Instant::now(),
        classes: Arc::new(classes),
        event_tx,
        metrics: metrics.clone(),
    };
    let info = state.node_info();
    if let Some(node_id) = &info.node_id {
        metrics.set_node_id(node_id.clone());
    }

    let listener_state = state.clone();
    let listener_socket = socket.clone();
    let listener = tokio::spawn(async move {
        crate::server::listen(listener_state, &listener_socket, socket_mode).await
    });

    // The full node ID and the bound addresses are what a peer needs.
    println!(
        "xlb node · {} · udp {} · {} class(es) · socket {}",
        info.node_id.as_deref().unwrap_or("no peer tier"),
        info.udp_addrs.join(","),
        info.classes.len(),
        socket
    );

    tokio::select! {
        signal = shutdown_signal() => signal?,
        // The listener only returns when it could not bind.
        listener = listener => listener.context("socket task")??,
    }
    println!("shutting down");
    // Close the endpoints before they drop, so peers see a clean close.
    for entry in state.classes.iter() {
        entry.class.shutdown().await;
    }
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        r = tokio::signal::ctrl_c() => r?,
        _ = term.recv() => {}
    }
    Ok(())
}

/// `--socket` beats `node.socket` beats the CLI default.
fn effective_socket(flag: Option<&str>, configured: Option<&str>) -> String {
    flag.or(configured)
        .map(str::to_string)
        .unwrap_or_else(crate::default_socket)
}

#[cfg(test)]
mod tests {
    use super::effective_socket;

    #[test]
    fn flag_beats_config_beats_default() {
        assert_eq!(effective_socket(Some("/f.sock"), Some("c.sock")), "/f.sock");
        assert_eq!(effective_socket(None, Some("c.sock")), "c.sock");
        assert_eq!(effective_socket(None, None), crate::default_socket());
    }
}
