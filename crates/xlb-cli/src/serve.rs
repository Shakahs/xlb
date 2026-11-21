//! `xlb serve` — register the configured classes and answer the socket.

use std::{num::NonZeroUsize, sync::Arc, time::{Duration, Instant}};

use anyhow::{Context, Result};
use tokio::sync::broadcast;
use tokio_util::task::AbortOnDropHandle;
use xlb::{AssetClass, AssetClassConfig, RequestLimits};

use crate::{
    config::NodeConfig,
    metrics_sink::MetricsSink,
    server::{ClassEntry, NodeState},
};

pub async fn run(config_path: &str, socket_flag: Option<&str>) -> Result<()> {
    let cfg = NodeConfig::load(config_path)?;
    let socket = effective_socket(socket_flag, cfg.node.socket.as_deref());
    let socket_mode = cfg.node.socket_mode()?;
    let request_limits = RequestLimits::new(
        cfg.node.total_request_concurrency,
        cfg.node.s3_request_concurrency,
        cfg.node.peer_request_concurrency,
    )?;

    // Built before the classes because every class's observer is a handle to
    // it; the node ID is filled in once a class has bound its endpoint.
    let (event_tx, _) = broadcast::channel(256);
    let metrics = Arc::new(MetricsSink::new(
        String::new(),
        cfg.node.peer_class.clone(),
        event_tx.clone(),
        cfg.node.metrics_path.as_deref(),
    )?);

    let concurrency = cfg.node.chunk_concurrency.get();
    let disk = if cfg.node.image_mirror_class.is_some() || cfg.node.snapshotter.is_some()
        || cfg.classes.iter().any(|class| class.model_mount.is_some())
    {
        Some(xlb_mount::DiskChunks::start(concurrency).await.context("start the io_uring chunk reader")?)
    } else {
        None
    };
    let mut classes = Vec::new();
    let mut model_sessions = Vec::new();
    for cc in &cfg.classes {
        // AssetClassConfig.name is &'static str; leak each name once at startup.
        let name: &'static str = Box::leak(cc.name.clone().into_boxed_str());
        let mut config = AssetClassConfig::new(name, cc.cache_dir());
        config.cache_budget_bytes = cc.cache_budget_bytes;
        config.peer = cc.peer_config()?;
        config.origin = cc.origin_config()?;
        config.request_limits = Some(request_limits.clone());
        config.observer = Some(metrics.observer());
        let has_origin = config.origin.is_some();
        let class = AssetClass::register(config)
            .await
            .with_context(|| format!("register class {}", cc.name))?;
        let model_mount = if let Some(root) = &cc.model_mount {
            let disk = disk.clone().context("model mount requires a chunk reader")?;
            let (mount, session) = xlb_mount::ModelMount::start(root, class.clone(), disk, concurrency).await?;
            model_sessions.push(session);
            Some(mount)
        } else {
            None
        };
        classes.push(ClassEntry {
            name: cc.name.clone(),
            class,
            has_origin,
            model_mount,
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

    let _reporter = AbortOnDropHandle::new(tokio::spawn(report_s3_requests(state.classes.clone())));
    // Keep enough room for a burst across concurrent pulls without retaining
    // request records indefinitely if the output sink is stalled.
    let request_log_capacity = NonZeroUsize::new(256).unwrap();
    let _request_loggers: Vec<_> = state.classes.iter().filter_map(|entry| {
        let mut requests = entry.class.subscribe_s3_requests(request_log_capacity)?;
        let class = entry.name.clone();
        Some(AbortOnDropHandle::new(tokio::spawn(async move {
            while let Some(request) = requests.recv().await {
                tracing::info!(%class, kind = ?request.kind, status = ?request.status,
                    elapsed_ms = request.elapsed.as_secs_f64() * 1_000.0, "XLB S3 request");
            }
        })))
    }).collect();
    let listener_state = state.clone();
    let listener_socket = socket.clone();
    let listener = tokio::spawn(async move {
        crate::server::listen(listener_state, &listener_socket, socket_mode).await
    });
    let mirror = async {
        let Some(name) = cfg.node.image_mirror_class.as_deref() else {
            return std::future::pending().await;
        };
        cfg.classes.iter().find(|class| class.name == name)
            .and_then(|class| class.origin.as_ref()).filter(|origin| origin.bucket.is_some())
            .with_context(|| format!("image mirror requires an S3 origin for class {name}"))?;
        let class = state.classes.iter().find(|entry| entry.name == name)
            .context("image mirror class is not registered")?;
        let disk = disk.clone().context("image mirror requires a chunk reader")?;
        crate::image_mirror::serve(class.class.clone(), concurrency, disk).await
    };
    let snapshotter = async {
        let Some(config) = cfg.node.snapshotter.clone() else {
            return std::future::pending().await;
        };
        let class = state.classes.iter().find(|entry| entry.name == config.class)
            .context("snapshotter class is not registered")?;
        let disk = disk.clone().context("snapshotter requires a chunk reader")?;
        xlb_mount::serve_snapshots(config, class.class.clone(), disk).await
    };

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
        result = mirror => result?,
        result = snapshotter => result?,
        // The listener only returns when it could not bind.
        listener = listener => listener.context("socket task")??,
    }
    println!("shutting down");
    drop(model_sessions);
    // Close the endpoints before they drop, so peers see a clean close.
    for entry in state.classes.iter() {
        entry.class.shutdown().await;
    }
    Ok(())
}

async fn report_s3_requests(classes: Arc<Vec<ClassEntry>>) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut previous = vec![xlb::S3RequestStats::default(); classes.len()];
    loop {
        interval.tick().await;
        for (entry, previous) in classes.iter().zip(&mut previous) {
            if let Some(stats) = entry.class.s3_request_stats().filter(|stats| *stats != *previous) {
                tracing::info!(class = %entry.name, active = stats.active, peak = stats.peak,
                    active_ranges = stats.active_ranges, peak_ranges = stats.peak_ranges,
                    dropped_records = stats.dropped_records,
                    "XLB S3 request concurrency");
                *previous = stats;
            }
        }
    }
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
