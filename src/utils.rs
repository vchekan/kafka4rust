use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use tracing::{debug, error};
use opentelemetry::global;
use tracing_attributes::instrument;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

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
pub fn init_tracer() {
    tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
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

