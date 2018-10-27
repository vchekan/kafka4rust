use connection::BrokerConnection;
use futures::future::{self, Future};
use std::io;
use protocol;
use protocol::write_request;
use std::net::SocketAddr;
use futures::sync::mpsc::SendError;
use tokio::prelude::*;
use tokio;

#[derive(Debug)]
pub struct Broker {
    //connection: Option<BrokerConnection>,
    /// (api_key, agreed_version)
    negotiated_api_version: Vec<(i16,i16)>,
    addr: SocketAddr,
    correlation_id: u32,
}

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
            negotiated_api_version: vec![],
            addr,
            correlation_id: 0,
        })
    }

    pub(crate) fn negotiate_api_versions(mut self) -> impl Future<Item=Self, Error=()> {
        let correlation_id = self.correlation_id;
        self.correlation_id += 1;
        BrokerConnection::connect(self.addr.clone()).
            and_then(|(connect_request_sender, connect_response_receiver)| {
                /*
                // Start connection response handling loop
                tokio::spawn(connect_response_receiver.for_each(|buf| {
                        println!("Received response");
                        future::ok(())
                }));
                connect_request_sender
            }).
            map(move |connect_request_sender| {
            */
                let request = protocol::ApiVersionsRequest0{};
                let mut buff = Vec::with_capacity(1024);
                write_request(&request, correlation_id, None, &mut buff);
                // TODO: handle result
                let _res = connect_request_sender.unbounded_send(buff);
                connect_response_receiver.split()
            })
    }

    fn set_api_compatibility(&mut self, versions: &protocol::ApiVersionsResponse0) {
        //
        // them:  mn----mx
        // me  :             mn-------mx
        // join:        mx < mn
        //
        // Empty join: max<min. For successful join: min<=max
        //
        let my_versions = protocol::supported_versions();
        self.negotiated_api_version =
            versions.api_versions.iter().map(|them| {
                match my_versions.iter().find(|(k,_,_)| {them.api_key == *k}) {
                    Some((k,mn,mx)) => {
                        let agreed_min = mn.max(&them.min_version);
                        let agreed_max = mx.min(&them.max_version);
                        if agreed_min <= agreed_max {
                            Some((*k, *agreed_max))
                        } else {
                            None
                        }
                    }
                    None => None
                }
            }).flatten().collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn negotiate_api_works() {
        let bootstrap = env::var("kafka-bootstrap").unwrap_or("localhost".to_string());
        let addr = format!("{}:9092", bootstrap);
        let mut broker = Broker::new(&addr).expect("Broker creating failed");
        println!("broker: {:?}", broker);

        let versions = broker.negotiate_api_versions().
            map(|br| {println!("Negotiated: {:#?}", br)});
            //map_err(|e| {println!("Error: {}", e)});

        tokio::run(versions);
    }
}
