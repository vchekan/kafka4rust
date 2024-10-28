use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use opentelemetry::{trace::Tracer, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace::Config, Resource};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tracing::{debug, error};
use tracing_attributes::instrument;
use tracing_futures::WithSubscriber;
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Resolve addresses and produce only those which were successfully resolved.
/// Unresolved entries will be logged with `error` level.
/// Input list is comma or space separated list of IP addresses or hostnames.
/// If port is missing, 9092 is assumed.
///
/// ```
/// let bootstrap = "127.0.0.1,192.169.1.1, 192.169.1.2, , 192.169.1.3:9099 localhost:1 www.ibm.com";
/// let result = bootstrap.to_bootstrap_addr()?;
/// ```
#[instrument(ret)]
pub(crate) fn resolve_addr(addr: &str) -> Vec<SocketAddr> {
    addr.split(|c| c == ',' || c == ' ')
        .filter(|a| !a.is_empty())
        .flat_map(|addr: &str| {
            if let Ok(addr) = addr.parse::<IpAddr>() {
                return vec![SocketAddr::new(addr, 9092)];
            }
            if let Ok(addr) = addr.parse::<SocketAddr>() {
                return vec![addr];
            }
            if let Ok(addr) = addr.to_socket_addrs() {
                debug!("to_socket_addrs() resolved {:?}", addr);
                return addr.collect();
            } else {
                debug!("to_socket_addrs() failed {}", addr);
            }
            if let Ok(addr) = (addr, 0).to_socket_addrs() {
                let mut addresses: Vec<SocketAddr> = addr.collect();
                for a in &mut addresses {
                    a.set_port(9092);
                }
                return addresses;
            }

            error!("Can't parse: '{}'", addr);
            vec![]
        })
        .collect()
}

pub fn init_console_tracer() -> tracing_subscriber::fmt::Layer<tracing_subscriber::layer::Layered<tracing_opentelemetry::OpenTelemetryLayer<tracing_subscriber::layer::Layered<EnvFilter, tracing_subscriber::Registry>, opentelemetry_sdk::trace::Tracer>, tracing_subscriber::layer::Layered<EnvFilter, tracing_subscriber::Registry>>>  {
    tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)    
}

pub fn init_grpc_opentetemetry_tracer() -> opentelemetry_sdk::trace::TracerProvider {
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter()
            //.tonic().with_endpoint("http://127.0.0.1:4317/")
            .http().with_endpoint("http://127.0.0.1:4318/v1/traces")
        )
        .with_trace_config(Config::default().with_resource(Resource::new(vec![KeyValue::new(
            SERVICE_NAME,
            "karst-cli",
        )])))
        // .install_batch(opentelemetry_sdk::runtime::Tokio)
        .install_simple()
        .expect("Unable to init tracing")
}

