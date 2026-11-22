//! `xlb serve` — register the configured classes and answer the socket.

use std::{num::NonZeroUsize, sync::Arc, time::{Duration, Instant}};

use anyhow::{Context, Result};
use tokio::sync::broadcast;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use xlb::{AssetClass, AssetClassConfig, RequestLimits};

use crate::{
    config::NodeConfig,
    metrics_sink::MetricsSink,
    server::{ClassEntry, NodeState},
    supervisor::{ServiceTask, supervise},
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
    let observed_limits = request_limits.clone();
    let _limit_reporter = AbortOnDropHandle::new(tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let stats = observed_limits.stats();
            tracing::info!(total = stats.total, s3 = stats.origin, peer = stats.peer,
                total_peak = stats.total_peak, s3_peak = stats.origin_peak, peer_peak = stats.peer_peak,
                "XLB request concurrency");
        }
    }));

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
    let chunk_limit = xlb::ChunkAdmission::new(concurrency);
    tracing::info!(chunk_limit = concurrency, total_limit = cfg.node.total_request_concurrency.get(),
        s3_limit = cfg.node.s3_request_concurrency.get(), peer_limit = cfg.node.peer_request_concurrency.get(),
        "XLB download limits");
    let mut classes = Vec::new();
    let mut model_sessions = Vec::new();
    for cc in &cfg.classes {
        // AssetClassConfig.name is &'static str; leak each name once at startup.
        let name: &'static str = Box::leak(cc.name.clone().into_boxed_str());
        let mut config = AssetClassConfig::new(name, cc.cache_dir());
        config.cache_budget_bytes = cc.cache_budget_bytes;
        if cc.talos_scratch_cache {
            let (memory, disk) = xlb::cas_cache::CasCache::talos_budgets(&cc.cache_dir())?;
            config.cas_memory_bytes = memory;
            config.cache_budget_bytes = disk;
            tracing::info!(class = %cc.name, memory, disk, "size XLB CAS from Talos scratch and RAM");
        } else if let Some(memory) = cc.cas_memory_bytes {
            config.cas_memory_bytes = memory;
        }
        config.peer = cc.peer_config()?;
        config.origin = cc.origin_config()?;
        config.request_limits = Some(request_limits.clone());
        config.chunk_limit = Some(chunk_limit.clone());
        config.observer = Some(metrics.observer());
        let has_origin = config.origin.is_some();
        let class = AssetClass::register(config)
            .await
            .with_context(|| format!("register class {}", cc.name))?;
        let model_mount = if let Some(root) = &cc.model_mount {
            let (mount, session) = xlb_mount::ModelMount::start(root, class.clone(), concurrency).await?;
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
    // The full node ID and the bound addresses are what a peer needs.
    println!(
        "xlb node · {} · udp {} · {} class(es) · socket {}",
        info.node_id.as_deref().unwrap_or("no peer tier"),
        info.udp_addrs.join(","),
        info.classes.len(),
        socket
    );

    let services = async {
        let cancel = CancellationToken::new();
        let listener = state.clone();
        let socket_stop = cancel.clone();
        let mut services = vec![ServiceTask::new("control socket", async move {
            crate::server::listen(listener, &socket, socket_mode, socket_stop).await
        })];
        if let Some(name) = cfg.node.image_mirror_class.as_deref() {
            cfg.classes.iter().find(|class| class.name == name)
                .and_then(|class| class.origin.as_ref())
                .with_context(|| format!("image mirror requires an S3 origin for class {name}"))?;
            let entry = state.classes.iter().find(|entry| entry.name == name)
                .context("image mirror class is not registered")?;
            services.push(ServiceTask::new("OCI mirror", crate::image_mirror::serve(
                entry.class.clone(), concurrency, cancel.clone(),
            )));
        }
        if let Some(config) = cfg.node.snapshotter {
            let entry = state.classes.iter().find(|entry| entry.name == config.class)
                .context("snapshotter class is not registered")?;
            services.push(ServiceTask::new("containerd snapshotter", xlb_mount::serve_snapshots(
                config, entry.class.clone(), concurrency, cancel.clone().cancelled_owned(),
            )));
        }
        if !model_sessions.is_empty() {
            services.push(ServiceTask::new("model mounts", xlb_mount::serve_model_sessions(
                model_sessions, cancel.clone().cancelled_owned(),
            )));
        }
        for entry in state.classes.iter() {
            let class = entry.class.clone();
            let stop = cancel.clone();
            services.push(ServiceTask::new(format!("{} cache", entry.name), async move {
                tokio::select! {
                    result = class.watch_cache() => Ok(result?),
                    () = stop.cancelled() => Ok(()),
                }
            }));
        }
        Ok::<_, anyhow::Error>((services, cancel))
    }.await;
    match services {
        Ok((services, cancel)) => {
            supervise(services, cancel, shutdown_signal(), close_storage(&state.classes)).await
        }
        Err(error) => {
            let _ = close_storage(&state.classes).await;
            Err(error)
        }
    }
}

async fn close_storage(classes: &[ClassEntry]) -> Result<()> {
    println!("shutting down");
    futures::future::join_all(classes.iter().map(|entry| entry.class.shutdown()))
        .await.into_iter().collect::<xlb::Result<()>>()?;
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
                    active_chunks = stats.active_chunks, peak_chunks = stats.peak_chunks,
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
