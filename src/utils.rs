use std::net::{SocketAddr, IpAddr, ToSocketAddrs};

/// Resolve adresses and produce only those which were successfully resolved.
/// Unresolved entries will be logged with `error` level.
/// Input list is comma or space separated list of IP addresses or hostnames.
/// If port is missing, 9092 is assummed.
/// 
/// ```
/// let bootstrap = "127.0.0.1,192.169.1.1, 192.169.1.2, , 192.169.1.3:9099 localhost:1 www.ibm.com";
/// let result = bootstrap.to_bootstrap_addr()?;
/// ```
pub (crate) fn resolve_addr(addr: &str) -> Vec<SocketAddr> {
    addr.split(|c| c == ',' || c == ' ').
        filter(|a| !a.is_empty()).
        flat_map(|addr: &str| {
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
        }).collect()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    pub(crate) fn init_test() -> Result<tokio::runtime::Runtime> {
        simple_logger::init_with_level(log::Level::Debug)?;
        let runtime = tokio::runtime::Builder::new().
            basic_scheduler().
            core_threads(2).
            thread_name("test_k4rs").build()?;
        Ok(runtime)
    }
}