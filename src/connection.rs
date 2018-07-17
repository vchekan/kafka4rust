use std::net::SocketAddr;
use tokio::net::{TcpStream, ConnectFuture};
use protocol::{write_request, read_response};

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
    use std::io::Cursor;
    use protocol;

    #[test]
    fn it_works() {
        let bootstrap = env::var("kafka-bootstrap").unwrap_or("localhost".to_string());
        println!("bootstrap: {}", bootstrap);
        let addr = format!("{}:9092", bootstrap);
        let addr = addr.to_socket_addrs().unwrap().next().expect(format!("Host '{}' not found", addr).as_str());
        let conn = BrokerConnection::new(&addr);
        tokio::run(
            conn.tcp.
            and_then(|tcp| {
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
                println!("Response: {:?}", buff);
                let mut cursor = Cursor::new(buff);
                let (corr_id, response) = read_response::<protocol::ApiVersionsResponse1>(&mut cursor);
                println!("CorrId: {}, Response: {:?}", corr_id, response);
                Ok(())
            }).
            map_err(|e| {
                println!("Failed. {:?}", e);
                ()
            })
        );
    }
}
