use connection::BrokerConnection;
use futures::future::Future;
use std::io;
use protocol;

#[derive(Debug)]
pub struct Broker {
    //state: BrokerState,
    connection: BrokerConnection,
}

/*#[derive(Debug)]
pub enum BrokerState {
    Initial,
    Connecting,
    Connected,
    Closed
}*/

#[derive(Debug, Fail)]
pub enum BrokerError {
    #[fail(display = "invalid broker address: '{}'", address)]
    InvalidBrokerAddress {
        address: String,
    },
}

impl Broker {
    pub fn new(address: &str) -> io::Result<Broker> {
        use std::net::ToSocketAddrs;
        let addr = address.to_socket_addrs()?.next().expect(format!("Host '{}' not found", address).as_str());
        Ok(Broker {
            //state: BrokerState::Initial,
            connection: BrokerConnection::new(addr)
        })
    }

    pub(crate) fn negotiate_api_versions(self) -> impl Future<Item=protocol::ApiVersionsResponse0,Error=String> {
        self.connection.connect2().
            map_err(|e| {e.to_string()}).
            and_then(|conn| {
                conn.request( protocol::ApiVersionsRequest0{})
            }).map(|(conn, corr_id, response)| {response})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn it_works() {
        let bootstrap = env::var("kafka-bootstrap").unwrap_or("localhost".to_string());
        let addr = format!("{}:9092", bootstrap);
        let broker = Broker::new(&addr).expect("Broker creating failed");
        println!("broker: {:?}", broker);

        let versions = broker.negotiate_api_versions().
            map(|resp| {println!("{:?}", resp)}).
            map_err(|e| {println!("Error: {}", e)});

        tokio::run(versions);
    }
}
