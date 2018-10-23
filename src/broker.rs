use connection::BrokerConnection;
use futures::future::Future;
use std::io;
use crate::protocol;
use tokio;

#[derive(Debug)]
pub struct Broker {
    connection: Option<BrokerConnection>,
    /// (api_key, agreed_version)
    api_version: Vec<(i16,i16)>,
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
        Ok(Broker {connection: Some(BrokerConnection::new(addr)), api_version: vec![]})
    }

    pub(crate) fn negotiate_api_versions(mut self) -> impl Future<Item=Self,Error=String> {
        let (mut self2, conn) = self.detach();
        conn.connect().
            map_err(|e| {e.to_string()}).
            and_then(|conn| {
                conn.request( protocol::ApiVersionsRequest0{})
            }).map(move |(conn, _corr_id, response)| {
                //self.connection.attach(conn);
                self2.set_api_compatibility(&response);
                self2.connection = Some(conn);
                self2
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
        self.api_version =
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

    fn detach(mut self) -> (Self,BrokerConnection) {
        let conn = self.connection;
        self.connection = None;
        (self, conn.unwrap())
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
        let broker = Broker::new(&addr).expect("Broker creating failed");
        println!("broker: {:?}", broker);

        let versions = broker.negotiate_api_versions().
            map(|br| {println!("Negotiated: {:#?}", br)}).
            map_err(|e| {println!("Error: {}", e)});

        tokio::run(versions);
    }
}
