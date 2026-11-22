use anyhow::Result;
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub struct Telemetry {
    traces: Option<SdkTracerProvider>,
    metrics: Option<SdkMeterProvider>,
    _console: tracing_appender::non_blocking::WorkerGuard,
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(provider) = &self.metrics { let _ = provider.shutdown(); }
        if let Some(provider) = &self.traces { let _ = provider.shutdown(); }
    }
}

pub fn init(daemon: bool) -> Result<Telemetry> {
    let enabled = daemon && std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok_and(|value| !value.is_empty());
    let resource = Resource::builder().with_service_name("xlb").build();
    let traces = if enabled {
        let exporter = opentelemetry_otlp::SpanExporter::builder().with_http().build()?;
        let provider = SdkTracerProvider::builder().with_resource(resource.clone()).with_batch_exporter(exporter).build();
        global::set_tracer_provider(provider.clone());
        Some(provider)
    } else { None };
    let metrics = if enabled {
        let exporter = opentelemetry_otlp::MetricExporter::builder().with_http().build()?;
        let provider = SdkMeterProvider::builder().with_resource(resource).with_periodic_exporter(exporter).build();
        global::set_meter_provider(provider.clone());
        Some(provider)
    } else { None };
    let span_layer = traces.as_ref().map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer("xlb")));
    let (console, guard) = if daemon {
        tracing_appender::non_blocking(std::io::stdout())
    } else {
        tracing_appender::non_blocking(std::io::stderr())
    };
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .with(tracing_subscriber::fmt::layer().with_ansi(false).with_writer(console))
        .with(span_layer)
        .try_init()?;
    Ok(Telemetry { traces, metrics, _console: guard })
}
