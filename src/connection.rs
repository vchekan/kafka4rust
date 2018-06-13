use std::net::SocketAddr;
use tokio::net::{TcpStream, ConnectFuture};

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

