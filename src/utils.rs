use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use log::{debug, error};
use opentelemetry::global;
use tracing_attributes::instrument;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

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

pub struct TraceGuard {}
impl Drop for TraceGuard {
    fn drop(&mut self) {
        global::shutdown_tracer_provider();
    }
}
pub fn init_tracer(name: &str) -> anyhow::Result<TraceGuard> {
    let exporter = opentelemetry_otlp::new_exporter().tonic();
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .install_simple()?;

    /*global::set_text_map_propagator(opentelemetry_jaeger::Propagator::default());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name(name)
        .install_simple()?;
    */
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(telemetry);
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(TraceGuard{})

    // let exporter = opentelemetry_jaeger::Exporter::builder()
    //     .with_agent_endpoint("localhost:6831".parse().unwrap())
    //     .with_process(opentelemetry_jaeger::Process {
    //         service_name: "kafka4rust".to_string(),
    //         tags: vec![
    //             //Key::new("exporter").string("jaeger"),
    //             //Key::new("float").f64(312.23),
    //         ],
    //     })
    //     .init()?;
    // let provider = sdk::Provider::builder()
    //     .with_simple_exporter(exporter)
    //     .with_config(sdk::Config {
    //         default_sampler: Box::new(sdk::Sampler::Always),
    //         max_events_per_span: 500,
    //         ..Default::default()
    //     })
    //     .build();
    // global::set_provider(provider);
    // Ok(())
}

#[derive(Debug)]
pub(crate) struct TracedMessage<T> {
    msg: T,
    trace: Option<tracing::Id>
}

impl <T> TracedMessage<T> {
    pub fn new(msg: T) -> Self {
        TracedMessage {
            msg,
            trace: tracing::Span::current().id()
        }
    }

    // pub fn follows_from(&self) {
    //     tracing::Span::current().follows_from(&self.trace);
    // }

    pub fn get(self) -> T {
        tracing::Span::current().follows_from(self.trace);
        self.msg
    }
}

// impl <T> Deref for TracedMessage<T> {
//     type Target = T;
//
//     fn deref(&self) -> &Self::Target {
//         tracing::Span::current().follows_from(&self.trace);
//         &self.msg
//     }
// }