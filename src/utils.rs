use crate::types::BrokerAddress;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace::Config, Resource};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use tracing::{debug, error};
use tracing_attributes::instrument;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

/// Resolve addresses and produce only those which were successfully resolved.
/// Unresolved entries will be logged with `error` level.
/// Input list is comma or space separated list of IP addresses or hostnames.
/// If port is missing, 9092 is assumed.
///
/// ```
/// let bootstrap = "127.0.0.1,192.169.1.1, 192.169.1.2, , 192.169.1.3:9099 localhost:1 kafka.company.com";
/// let result = bootstrap.to_bootstrap_addr()?;
/// ```
#[instrument(ret)]
pub(crate) fn resolve_addr(addr: &str) -> Vec<BrokerAddress> {
    addr.split(|c| c == ',' || c == ' ')
        .filter(|a| !a.is_empty())
        .flat_map(|addr: &str| {
            if let Ok(addr) = addr.parse::<IpAddr>() {
                return vec![BrokerAddress {
                    addr: SocketAddr::new(addr, 9092),
                    hostname: addr.to_string(),
                }];
            }
            if let Ok(addr) = addr.parse::<SocketAddr>() {
                return vec![BrokerAddress {
                    addr,
                    hostname: addr.ip().to_string(),
                }];
            }
            if let Ok(addrs) = addr.to_socket_addrs() {
                debug!("to_socket_addrs() resolved {:?}", addr);
                return addrs
                    .map(|sa| BrokerAddress {
                        addr: sa,
                        hostname: sa.ip().to_string(),
                    })
                    .collect();
            } else {
                debug!("to_socket_addrs() failed {}", addr);
            }
            if let Ok(addrs) = (addr, 0).to_socket_addrs() {
                let addresses: Vec<BrokerAddress> = addrs
                    .map(|sa| BrokerAddress {
                        addr: SocketAddr::new(sa.ip(), 9092),
                        hostname: sa.ip().to_string(),
                    })
                    .collect();
                return addresses;
            }

            error!("Can't parse: '{}'", addr);
            vec![]
        })
        .collect()
}

pub fn init_console_tracer() -> tracing_subscriber::fmt::Layer<
    tracing_subscriber::layer::Layered<
        tracing_opentelemetry::OpenTelemetryLayer<
            tracing_subscriber::layer::Layered<EnvFilter, tracing_subscriber::Registry>,
            opentelemetry_sdk::trace::Tracer,
        >,
        tracing_subscriber::layer::Layered<EnvFilter, tracing_subscriber::Registry>,
    >,
> {
    tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
}

pub fn init_grpc_opentetemetry_tracer() -> opentelemetry_sdk::trace::TracerProvider {
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                //.tonic().with_endpoint("http://127.0.0.1:4317/")
                .http()
                .with_endpoint("http://127.0.0.1:4318/v1/traces"),
        )
        .with_trace_config(
            Config::default().with_resource(Resource::new(vec![KeyValue::new(
                SERVICE_NAME,
                "karst-cli",
            )])),
        )
        // .install_batch(opentelemetry_sdk::runtime::Tokio)
        .install_simple()
        .expect("Unable to init tracing")
}
