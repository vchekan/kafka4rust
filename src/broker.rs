use connection::BrokerConnection;
//use futures::future::Future;

use failure::Error;

#[derive(Debug)]
pub struct Broker {
    state: BrokerState,
    connection: BrokerConnection,
}

#[derive(Debug)]
pub enum BrokerState {
    Initial,
    Connecting,
    Connected,
    Closed
}

#[derive(Debug, Fail)]
pub enum BrokerError {
    #[fail(display = "invalid broker address: '{}'", address)]
    InvalidBrokerAddress {
        address: String,
    },
}

impl Broker {
    pub fn new(address: &str) -> Result<Broker,Error> {
        use std::net::ToSocketAddrs;
        let addr = address.to_socket_addrs()?.next().expect(format!("Host '{}' not found", address).as_str());
        Ok(Broker {
            state: BrokerState::Initial,
            connection: BrokerConnection::new(&addr)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    //use std::net::ToSocketAddrs;
    //use tokio;
    use std::env;

    #[test]
    fn it_works() {
        let bootstrap = env::var("kafka-bootstrap").unwrap_or("localhost".to_string());
        println!("bootstrap: {}", bootstrap);
        let addr = format!("{}:9092", bootstrap);
        let broker = Broker::new(&addr);
        println!("broker: {:?}", broker);

        //let kafkaClient =

        //tokio::run(kafkaClient);
    }
}
