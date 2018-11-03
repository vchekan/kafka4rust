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
    sync::mpsc,
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
    /*pub fn new(addr: SocketAddr, rx: mpsc::UnboundedReceiver<Vec<u8>>) -> BrokerConnection {
        BrokerConnection {
            correlationId: 0,
            addr,
            //tcp: None
            rx
        }
    }*/

    /*pub fn from_broker(broker: &protocol::Broker) -> Option<BrokerConnection> {
        Self::from_host(&broker.host, broker.port as u16).
            map(|addr|{
                BrokerConnection {
                    correlationId: 0,
                    addr,
                    tcp: None
                }
            })
    }*/

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
    pub fn connect(addr: SocketAddr) -> impl Future<Item=Self, Error=()> {
        TcpStream::connect(&addr).
            // TODO: prevent thread from dying
            map_err(move |e| { println!("Error {}", e); }).
            map(move |tcp| {
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
        let (mut conn, tcp) = self.detach();
        write_all(tcp, buf).
        and_then(|(tcp, mut buf)| {
            // Read length into buffer
            buf.resize(4, 0_u8);
            // TODO: ensure length is sane
            read_exact(tcp, buf)
        }).and_then(|(tcp, mut buf)|{
            let len = BigEndian::read_u32(&buf);
            println!("Response len: {}", len);
            buf.resize(len as usize, 0_u8);
            read_exact(tcp, buf)
        }).map(move |(tcp, buf)| {
            conn.tcp = Some(tcp);
            (conn,buf)
        }).
            map_err(|e| { e.description().to_string()})
    }
}

/*
impl Stream for BrokerConnection {
    type Item = Vec<u8>;
    type Error = String;

    fn poll(&mut self) -> Result<Async<Option<<Self as Stream>::Item>>, <Self as Stream>::Error> {
        match self.msg_rx.poll() {
            Ok(Async::NotReady) => self.poll_response(),
            Err(e) => Err(e),
            Ok(Option(buff)) => {
                match self.tcp.read_buf() {
                    
                };
                self.poll_response()
            },
        }
    }
}
*/

//impl Connected {
/*    fn request<R>(mut self, request: R) -> impl Future<Item=(Self,u32,R::Response), Error=String>
        where R: protocol::Request
    {
        // TODO: buffer management
        let mut buff = Vec::with_capacity(1024);
        write_request(&request, self.correlation_id, None, &mut buff);
        self.correlation_id += 1;

        let tcp = self.tcp;
        write_all(self.tcp, buff);
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
            // TODO: check correlationId
            // TODO: check for response error
            println!("CorrId: {}, Response: {:#?}", corr_id, response);
            (tcp, corr_id, response)
        })
    }
}
*/

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
