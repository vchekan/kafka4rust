use tokio::prelude::*;
use broker::Broker;
use futures::future::*;
use connection::BrokerConnection;
use tokio::net::TcpStream;
use std::error::Error;
use protocol::{self, write_request, read_response};
use tokio::io::{write_all, read_exact};
use std::io::Cursor;
use byteorder::BigEndian;
use bytes::ByteOrder;
use std::net::SocketAddr;

#[derive(Debug)]
pub struct Cluster {
    brokers: Vec<protocol::Broker>,
}



impl Cluster {
    pub fn bootstrap(bootstrap: Vec<String>, topics: &[&str]) -> impl Future<Item=Self, Error=String> {
        // copy topics
        let topics = topics.iter().map(|s| {s.to_string()}).collect();

        let bootstraps = bootstrap.iter().
            map(|addr|{ Broker::connect(&addr) });


        select_ok(bootstraps).
        // TODO: move it to BrokerConnection
        and_then(|(broker,_)| {
            debug!("Connected to {:?}", broker);
            let request = protocol::MetadataRequest0{topics};
            broker.request(&request)
        }).map(move |(broker, response)| {
            debug!("Metadata response: {:?}", response);
            Cluster { brokers: response.brokers }
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    use futures::future::Future;
    use simplelog::*;

    #[test]
    fn resolve() {
        CombinedLogger::init(vec![TermLogger::new(LevelFilter::Debug, Config::default()).unwrap()]);
        debug!("Starting test");
        let cluster = Cluster::bootstrap(vec!["127.0.0.1:9092".to_string()], &vec!["t1"]).
            map(|cluster| {
                info!("Bootstrapped: {:?}", cluster);
            }).
            map_err(|e| {
                error!("Bootstrap failed: {}", e);
            });

        tokio::run(cluster);
    }
}