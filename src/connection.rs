use byteorder::BigEndian;
use bytes::ByteOrder;
use std::io;
use std::net::SocketAddr;

use futures::io::{AsyncWriteExt, AsyncReadExt};
use romio::tcp::TcpStream;

#[derive(Debug)]
pub struct BrokerConnection {
    correlation_id: u32,
    addr: SocketAddr,
    tcp: TcpStream,
}

impl BrokerConnection {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let tcp = await!(romio::tcp::TcpStream::connect(&addr))?;
        Ok(BrokerConnection {
            correlation_id: 0,
            addr,
            tcp,
        })
    }

    pub async fn request<'a>(&'a mut self, mut buf: &'a mut Vec<u8>) -> io::Result<()> {
        debug!("Sending request[{}]", buf.len());
        await!(self.tcp.write_all(&buf))?;
        debug!("Sent request, reading length...");
        // Read length into buffer
        buf.resize(4, 0_u8);
        // TODO: ensure length is sane
        await!(self.tcp.read_exact(&mut buf))?;
        let len = BigEndian::read_u32(&buf);
        debug!("Response len: {}, reading body...", len);
        buf.resize(len as usize, 0_u8);
        await!(self.tcp.read_exact(&mut buf))?;
        debug!("Read body [{}]", buf.len());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::net::ToSocketAddrs;
    use futures::executor;
    use simplelog::*;

    #[test]
    fn it_works() {
        CombinedLogger::init(vec![
            TermLogger::new(LevelFilter::Debug, Config::default()).unwrap()
        ]).unwrap();

        let bootstrap = env::var("kafka-bootstrap").unwrap_or("localhost:9092".to_string());
        println!("bootstrap: {}", bootstrap);
        let addr = bootstrap
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect(format!("Host '{}' not found", bootstrap).as_str());

        executor::block_on(async {
            let conn = await!(BrokerConnection::connect(addr)).unwrap();
            info!("res: {:?}", conn);
            ()
        });
    }
}
