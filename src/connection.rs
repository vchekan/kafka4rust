use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use tokio::{
    self,
    prelude::*,
    net::{TcpStream},
    io::{write_all, read_exact}
};
use futures::{
    future::Future,
};
use protocol;
use protocol::{write_request, read_response};
use std::error::Error;
use byteorder::BigEndian;
use bytes::ByteOrder;
use std::io::Cursor;

#[derive(Debug)]
pub struct BrokerConnection {
    correlation_id: u32,
    addr: SocketAddr,
    tcp: Option<TcpStream>,
}

impl BrokerConnection {
    pub fn from_host(host: &str, port: u16) -> Option<SocketAddr> {
        match host.to_socket_addrs() {
            Ok(addr) => addr.into_iter().next(),
            Err(e) => {
                println!("Error resolving '{}' {}", host, e.to_string());
                None
            }
        }
    }

    /// Returns (cmd_sender, response_receiver)
    pub fn connect(addr: SocketAddr) -> impl Future<Item=Self, Error=String> {
        TcpStream::connect(&addr).
            // TODO: prevent thread from dying
            map_err(move |e| { e.description().to_string() }).
            map(move |tcp| {
                debug!("Connected to {:?}", tcp);
                BrokerConnection {
                    correlation_id: 0,
                    addr,
                    tcp: Some(tcp),
                }
            })
    }

    fn detach(mut self) -> (Self, TcpStream) {
        let tcp = self.tcp;
        self.tcp = None;
        (self, tcp.unwrap())
    }

    pub fn request(mut self, buf: Vec<u8>) -> impl Future<Item=(Self,Vec<u8>), Error=String> {
        debug!("Sending request[{}]", buf.len());
        let (mut conn, tcp) = self.detach();
        write_all(tcp, buf).
        and_then(|(tcp, mut buf)| {
            debug!("Sent request, reading length...");
            // Read length into buffer
            buf.resize(4, 0_u8);
            // TODO: ensure length is sane
            read_exact(tcp, buf)
        }).and_then(|(tcp, mut buf)|{
            let len = BigEndian::read_u32(&buf);
            debug!("Response len: {}, reading body...", len);
            buf.resize(len as usize, 0_u8);
            read_exact(tcp, buf)
        }).map(move |(tcp, buf)| {
            debug!("Read body [{}]", buf.len());
            conn.tcp = Some(tcp);
            (conn,buf)
        }).
            map_err(|e| { e.description().to_string()})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::ToSocketAddrs;
    use tokio;
    use std::env;

    #[test]
    fn it_works() {
        let bootstrap = env::var("kafka-bootstrap").unwrap_or("localhost:9092".to_string());
        println!("bootstrap: {}", bootstrap);
        let addr = bootstrap.to_socket_addrs().unwrap().next().expect(format!("Host '{}' not found", bootstrap).as_str());

        let conn = BrokerConnection::connect(addr.clone());
        /*tokio::run(
        conn.connect().
            map_err(|e| {
                println!("Failed. {:?}", e);
                ()
            }).map(|tcp| {
                println!("Connected: {:?}", tcp)
            })
        );*/
    }
}
