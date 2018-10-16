use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use tokio::net::{TcpStream};
use tokio::io::{write_all, read_exact};
use futures::{
    future::{Future},
};
use std::io;
use protocol;
use protocol::{write_request, read_response};
use std::error::Error;
use byteorder::BigEndian;
use bytes::ByteOrder;
use std::io::Cursor;

#[derive(Debug)]
pub struct BrokerConnection {
    correlationId: u32,
    addr: SocketAddr,
}

//
// States
//
pub struct Connected {
    conn: BrokerConnection,
    // TODO: this is None when tcp is moved into future
    // and attached back when future is complete.
    tcp: Option<TcpStream>,
}

impl BrokerConnection {
    pub fn new(addr: SocketAddr) -> BrokerConnection {
        BrokerConnection {
            correlationId: 0,
            addr,
        }
    }

    pub fn from_broker(broker: &protocol::Broker) -> Option<BrokerConnection> {
        Self::from_host(&broker.host, broker.port as u16).
            map(|addr|{
                BrokerConnection {
                    correlationId: 0,
                    addr
                }
            })
    }

    pub fn from_host(host: &str, port: u16) -> Option<SocketAddr> {
        match host.to_socket_addrs() {
            Ok(addr) => addr.into_iter().next(),
            Err(e) => {
                println!("Error resolving '{}' {}", host, e.to_string());
                None
            }
        }
    }

    pub fn connect(&self) -> impl Future<Item=TcpStream, Error=io::Error> {
        TcpStream::connect(&self.addr)
    }

    pub fn connect2(self) -> impl Future<Item=Connected, Error=io::Error> {
        TcpStream::connect(&self.addr).
            map(move |tcp| {Connected{conn: self, tcp: Some(tcp)}})
    }
}

impl Connected {
    pub fn request<R>(mut self, request: R) -> impl Future<Item=(Self,u32,R::Response), Error=String>
        where R: protocol::Request
    {
        // TODO: buffer management
        let mut buff = Vec::with_capacity(1024);
        write_request(&request, self.conn.correlationId, None, &mut buff);
        self.conn.correlationId += 1;

        let (mut conn, tcp) = self.detach();

        write_all(tcp, buff).
        map_err(|e| {e.description().to_string()}).
        and_then(|(tcp, mut buff)| {
            println!("Written");
            // Read length into buffer
            buff.resize(4, 0_u8);
            // TODO: ensure length is sane
            let tcp = read_exact(tcp, buff).
                map_err(|e| {e.description().to_string()});
            tcp
        }).and_then(|(tcp, mut buff)| {
            let len = BigEndian::read_u32(&buff);
            println!("Response len: {}", len);
            buff.resize(len as usize, 0_u8);
            read_exact(tcp, buff).
                map_err(|e| { e.description().to_string()})
        }).map(|(tcp, buff)| {
            let mut cursor = Cursor::new(buff);
            let (corr_id, response) = read_response::<R::Response>(&mut cursor);
            println!("CorrId: {}, Response: {:#?}", corr_id, response);
            // Re-attach tcp to logical connection
            conn.tcp = Some(tcp);
            (conn, corr_id, response)
        })
    }

    fn detach(mut self) -> (Self, TcpStream) {
        let tcp = self.tcp;
        self.tcp = None;
        (self, tcp.unwrap())
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

        let conn = BrokerConnection::new(addr);
        tokio::run(
        conn.connect().
            map_err(|e| {
                println!("Failed. {:?}", e);
                ()
            }).map(|tcp| {
                println!("Connected: {:?}", tcp)
            })
        );
    }
}
