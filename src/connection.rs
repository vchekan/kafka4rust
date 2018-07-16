use std::net::SocketAddr;
use tokio::net::{TcpStream, ConnectFuture};
use bytes::BufMut;
use protocol::{write_request, Request};

#[derive(Debug)]
pub struct BrokerConnection {
    tcp: ConnectFuture,
    state: ConnectionState
}

#[derive(Debug)]
enum ConnectionState {
    Connecting,
    Connected,
    Closing,
    Closed
}

impl BrokerConnection {
    pub fn new(addr: &SocketAddr) -> BrokerConnection {
        BrokerConnection {
            tcp: TcpStream::connect(addr),
            state: ConnectionState::Connecting
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::ToSocketAddrs;
    use tokio;
    use std::env;
    use tokio::prelude::*;
    use tokio::io::{write_all, read_exact};
    use byteorder::{ByteOrder, BigEndian};
    use protocol::ListGroupRequest;

    #[test]
    fn it_works() {
        let bootstrap = env::var("kafka-bootstrap").unwrap_or("localhost".to_string());
        println!("bootstrap: {}", bootstrap);
        let addr = format!("{}:9092", bootstrap);
        let addr = addr.to_socket_addrs().unwrap().next().expect(format!("Host '{}' not found", addr).as_str());
        let conn = BrokerConnection::new(&addr);
        let res = tokio::run(
            conn.tcp.
            and_then(|tcp| {
                println!("Connected!");

                let mut buff = Vec::with_capacity(1024);
                let request = Request::ListGroup(ListGroupRequest{});
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
            }).and_then(|(tcp, mut buff)| {
                println!("Response: {:?}", buff);
                Ok(())
            }).
            map_err(|e| {
                println!("Failed. {:?}", e);
                ()
            })
        );
    }
}
