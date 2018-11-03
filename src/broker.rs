use connection::BrokerConnection;
use futures::future::{self, Future};
use std::io;
use protocol;
use protocol::{write_request, read_response};
use std::net::*;
use futures::sync::mpsc::SendError;
use tokio::prelude::*;
use tokio;
use tokio::io::{write_all, read_exact};
use std::io::Cursor;

#[derive(Debug)]
pub struct Broker {
    /// (api_key, agreed_version)
    negotiated_api_version: Vec<(i16,i16)>,
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
    /*pub fn new(address: &str) -> io::Result<Broker> {
        use std::net::ToSocketAddrs;
        let addr = address.to_socket_addrs()?.next().expect(format!("Host '{}' not found", address).as_str());
        Ok(Broker {
            negotiated_api_version: vec![],
            addr,
        })
    }*/

    pub(crate) fn connect(addr: &str) -> impl Future<Item=Self, Error=()> {
        let addr: SocketAddr = addr.parse().expect("Can't parse address");
        BrokerConnection::connect(addr.clone()).
            and_then(|conn| {
                let req = protocol::ApiVersionsRequest0 {};
                let mut buf = Vec::with_capacity(1024);
                // TODO: This is special case, we need correlationId and clientId before broker is created...
                let correlation_id = 0;
                let client_id = None;

                write_request(&req, correlation_id, client_id, &mut buf);
                conn.request(buf).
                    map_err(|e| println!("Error {}", e))
            }).map(|(conn, buf)| {
                let mut cursor = Cursor::new(buf);
                let (corr_id, response) = read_response::<protocol::ApiVersionsResponse0>(&mut cursor);
                let negotiated_api_version = Broker::get_api_compatibility(&response);
                Broker {negotiated_api_version, correlation_id: 1 }
            })

    }

    /*
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
                connect_response_receiver.into_future().map(|(buff, receiver)| {

                })
            })
    }
    */

    fn get_api_compatibility(them: &protocol::ApiVersionsResponse0) -> Vec<(i16,i16)> {
        //
        // them:  mn----mx
        // me  :             mn-------mx
        // join:        mx < mn
        //
        // Empty join: max<min. For successful join: min<=max
        //
        let my_versions = protocol::supported_versions();
            them.api_versions.iter().map(|them| {
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
            }).flatten().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn negotiate_api_works() {
        let bootstrap = env::var("kafka-bootstrap").unwrap_or("127.0.0.1".to_string());
        let addr = format!("{}:9092", bootstrap);

        let versions = Broker::connect(&addr).
            map(|broker| {
                //broker.negotiate_api_versions()
                println!("Broker connected: {:?}", broker)
            });

        tokio::run(versions);
    }
}
