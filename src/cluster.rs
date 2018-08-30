use tokio::prelude::*;
use broker::Broker;
use futures::future::*;
use connection::BrokerConnection;
use tokio::net::TcpStream;
use std::error::Error;
use protocol::{MetadataRequest0};
use protocol::{write_request, read_response, MetadataResponse0};
use tokio::io::{write_all, read_exact};
use std::io::Cursor;
use byteorder::BigEndian;
use bytes::ByteOrder;
use std::net::SocketAddr;

pub struct Cluster {
    brokers: Vec<Broker>,
    bootstrap: Vec<String>,
}



/*fn bootstrap_actions(host: &str, resolver: &ResolverFuture) -> impl Future {
    resolver.lookup_ip(host).
    map(|resp| {
        let ip = resp.iter().next().unwrap();
        // TODO:
        let port = 9092;
        let conn = BrokerConnection::new(SocketAddr::new(ip, port));
        //conn.tcp
        ()
    })
}*/

impl Cluster {
    // TODO: more input types via trait ToBootstrap
    pub fn new(bootstrap: Vec<String>) -> Cluster {
        Cluster {bootstrap, brokers: vec![]}
    }

    // TODO: Error: figure out how to use "failure"
    pub fn bootstrap(&self, topics: &[&str]) -> impl Future<Item=MetadataResponse0, Error=String> {
        // copy topics
        let topics = topics.iter().map(|s| {s.to_string()}).collect();

        // TODO: port hardcoded
        let bootstraps: Vec<SocketAddr> = self.bootstrap.iter().
            filter_map(|host| {
                BrokerConnection::from_host(host, 9092_u16)
            }).collect();

        println!("bootstraps: {:?}", bootstraps);

        let bootstraps = bootstraps.iter().map(|addr|{
            TcpStream::connect(&addr).map_err(|e| {
                e.description().to_string()
            })
        });


        select_ok(bootstraps).
            /*and_then(|(addr,_)|{
                TcpStream::connect(&addr).map_err(|e| {
                    e.description().to_string()
                })
            }). */
        /*resolver.and_then(move |r| {
            let bootstrap = names.iter().
                map(|host|{ r.lookup_ip(host) });
            select_ok(bootstrap)
        }).map_err(|e| {
            e.description().to_string()
        }).
        and_then(|(resp, _)| {
            let addr = resp.iter().next().unwrap();
            // TODO: port
            let addr = SocketAddr::new(addr, 9092);
            println!("resolved: {:?}", addr);
            TcpStream::connect(&addr).map_err(|e| {
                e.description().to_string()
            })
        }).
        */
        and_then(|(tcp,_)| {
            println!("connected");
            // TODO: buffer management
            let mut buff = Vec::with_capacity(1024);
            let request = MetadataRequest0{topics};
            // TODO: correlation
            write_request(&request, 11, None, &mut buff);
            write_all(tcp, buff).map_err(|e| {e.description().to_string()})
        }).
        and_then(|(tcp, mut buff)|{
            println!("Written");
            buff.resize(4, 0_u8);
            read_exact(tcp, buff).
                map_err(|e| {e.description().to_string()})
        }).and_then(|(tcp, mut buff)| {
            let len = BigEndian::read_u32(&buff);
            println!("Response len: {}", len);
            buff.resize(len as usize, 0_u8);
            read_exact(tcp, buff).
                map_err(|e| {e.description().to_string()})
        }).map(|(tcp, buff)| {
            let mut cursor = Cursor::new(buff);
            let (corr_id, response) = read_response::<MetadataResponse0>(&mut cursor);
            println!("CorrId: {}, Response: {:#?}", corr_id, response);
            response
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    use futures::future::Future;

    #[test]
    fn resolve() {
        let p = future::lazy(||{
            // TODO: host as Ip does not work
            let mut cluster = Cluster::new(vec!["localhost:9092".to_string()]);
            let bs = cluster.bootstrap(&vec!["t1"]).
                map(|x: MetadataResponse0| {
                    println!("Resolved: {:?}", x);
                }).
                map_err(|e| {
                    println!("Resolve failed: {}", e);
                });
            bs
        });

        tokio::run(p);
    }
}