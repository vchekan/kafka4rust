use std::net::SocketAddr;
use tokio::net::{TcpStream};
use protocol::{Response};
use futures::{
    future::{Future},
    stream::{Stream}
};
use std::io;
use protocol;
use std::net::ToSocketAddrs;

#[derive(Debug)]
pub struct BrokerConnection {
    correlationId: u32,
    addr: SocketAddr,
    //state: ConnectionState
}

#[derive(Debug)]
enum ConnectionState {
    Connecting(SocketAddr),
    Connected(SocketAddr,TcpStream),
    Repairing(SocketAddr),
    Closing,
    Closed
}

#[derive(Debug)]
enum ConnectionEvent {
    Connected
}

enum ConnResponse {
    Event(ConnectionEvent),
    Response(Response),
}

impl BrokerConnection {
    pub fn new(addr: SocketAddr) -> BrokerConnection {
        BrokerConnection {
            correlationId: 0,
            addr,
            //state: ConnectionState::Connecting
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

    fn connect(&self) -> impl Future<Item=TcpStream, Error=io::Error> {
        TcpStream::connect(&self.addr)
    }

    /*
    pub fn state(addr: SocketAddr, commands: impl Stream<Item=Request, Error=()>) -> impl Stream<Item=ConnectionEvent, Error=io::Error> {
        stream::unfold(ConnectionState::Connecting(addr), move |state| {
            match state {
                ConnectionState::Connecting(addr) => {
                    let connected = TcpStream::connect(&addr).
                        map(move |tcp| { (ConnectionEvent::Connected, ConnectionState::Connected(addr, tcp)) });
                    Some(connected)
                }
                ConnectionState::Connected(addr) => {

                }
                _ => {
                    panic!("Not implemented yet")
                }
            }
        }).select(commands).map(|x| {

        })
    }

    pub fn execute(&mut self, request: Request) -> impl Future<Item=Response, Error=()> {
        self.
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
        //let addr = format!("{}:9092", bootstrap);
        let addr = bootstrap.to_socket_addrs().unwrap().next().expect(format!("Host '{}' not found", bootstrap).as_str());
        /*let conn = BrokerConnection::new(addr);
        tokio::run(
            conn.connect().
                map_err(|e| {println!("Connection error: {}", e)}).
                for_each(|evt| {
                    println!("Connection state: {:?}", evt);
                    future::ok(())
                })
                //into_future().
        );
        */

        let conn = BrokerConnection::new(addr);
        tokio::run(
        conn.connect().
            /*and_then(|tcp| {
                println!("Connected!");

                // TODO: use some size-bound structure to serialize and fail if exceed buffer size
                let mut buff = Vec::with_capacity(1024);
                let request = protocol::ApiVersionsRequest{};
                    //ListGroupRequest0{};
                //let r2 = MetadataRequest0{topic_name: vec!["test1".to_string(), "test2".to_string()]};
                write_request(&request, 11, None, &mut buff);
                write_all(tcp, buff)
            }).and_then(|(tcp, mut buff)| {
                println!("Written");
                buff.resize(4, 0_u8);
                read_exact(tcp, buff)
            }).and_then(|(tcp, mut buff)| {
                let len = BigEndian::read_u32(&buff);
                println!("Response len: {}", len);
                buff.resize(len as usize, 0_u8);
                read_exact(tcp, buff)
            }).and_then(|(tcp, buff)| {
                let mut cursor = Cursor::new(buff);
                let (corr_id, response) = read_response::<protocol::ApiVersionsResponse1>(&mut cursor);
                println!("CorrId: {}, Response: {:#?}", corr_id, response);
                Ok(())
            }).*/
            map_err(|e| {
                println!("Failed. {:?}", e);
                ()
            }).map(|tcp| {
                println!("Connected: {:?}", tcp)
            })
        );
    }
}
