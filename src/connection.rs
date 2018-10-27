use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use tokio::prelude::*;
use tokio::net::{TcpStream};
use tokio::io::{write_all, read_exact};
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
    pub fn connect(addr: SocketAddr) -> impl Future<Item=(mpsc::UnboundedSender<Vec<u8>>, mpsc::UnboundedReceiver<Vec<u8>>), Error=()> {
        let (cmd_sender, cmd_receiver) = mpsc::unbounded();
        let (response_sender, response_receiver) = mpsc::unbounded();
        TcpStream::connect(&addr).
            // TODO: prevent thread from dying
            map_err(move |e| {println!("Error {}", e);}).
            map(move |tcp| {
                let conn = BrokerConnection {
                    correlation_id: 0,
                    addr,
                };
                tokio::spawn(
                    cmd_receiver.for_each(move |buff| {
                        println!("Got buffer");
                        // TODO: manage result
                        let _res = response_sender.unbounded_send(buff);
                        future::ok(())
                    })
                );
                (cmd_sender, response_receiver)
            })
    }
//}

//impl Connected {
/*    pub fn request<R>(mut self, request: R) -> impl Future<Item=(Self,u32,R::Response), Error=String>
        where R: protocol::Request
    {
        // TODO: buffer management
        let mut buff = Vec::with_capacity(1024);
        write_request(&request, self.correlationId, None, &mut buff);
        self.correlationId += 1;

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
            // TODO: check for response error
            println!("CorrId: {}, Response: {:#?}", corr_id, response);
            (conn, corr_id, response)
        })
    }
*/
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
