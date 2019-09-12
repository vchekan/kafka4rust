use byteorder::BigEndian;
use bytes::ByteOrder;
use std::io;
use std::net::SocketAddr;
use crate::protocol::*;
use bytes::{Buf, IntoBuf};

use futures::io::{AsyncReadExt, AsyncWriteExt};
use romio::tcp::TcpStream;
use std::future::Future;
use std::task::{Context, Poll, Waker};
use futures::Stream;
use std::pin::Pin;

#[derive(Debug)]
pub struct BrokerConnection {
    //correlation_id: u32,
    addr: SocketAddr,
    tcp: TcpStream,
}

impl BrokerConnection {
    /// Connect to address but do not perform any check beyond successful tcp connection.
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let tcp = romio::tcp::TcpStream::connect(&addr).await?;
        Ok(BrokerConnection {
            //correlation_id: 0,
            addr,
            tcp,
        })
    }

    pub async fn request<'a>(&'a mut self, mut buf: &'a mut Vec<u8>) -> io::Result<()> {
        debug!("Sending request[{}]", buf.len());
        self.tcp.write_all(&buf).await?;
        debug!("Sent request, reading length...");
        // Read length into buffer
        buf.resize(4, 0_u8);
        // TODO: ensure length is sane
        self.tcp.read_exact(&mut buf).await?;
        let len = BigEndian::read_u32(&buf);
        debug!("Response len: {}, reading body...", len);
        buf.resize(len as usize, 0_u8);
        self.tcp.read_exact(&mut buf).await?;
        debug!("Read body [{}]", buf.len());
        Ok(())
    }

    async fn read(&self) {

    }
}

impl Stream for BrokerConnection {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buf = Vec::new();
        let(read, write) = self.tcp.split();
        read.poll()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor;
    use simplelog::*;
    use std::env;
    use std::net::ToSocketAddrs;
    use std::io::Cursor;

    #[test]
    fn it_works() {
        CombinedLogger::init(vec![
            TermLogger::new(LevelFilter::Debug, Config::default(), TerminalMode::Mixed).unwrap()
        ])
        .unwrap();

        let bootstrap = env::var("kafka-bootstrap").unwrap_or("localhost:9092".to_string());
        let addr = bootstrap
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect(format!("Host '{}' not found", bootstrap).as_str());

        executor::block_on(
            async {
                let mut conn = BrokerConnection::connect(addr).await.unwrap();
                info!("conn: {:?}", conn);
                let request = ApiVersionsRequest0 {};
                let mut buff = Vec::new();
                write_request(&request, 0, None, &mut buff);
                conn.request(&mut buff).await.unwrap();
                let (correlationId, versions) : (_, ApiVersionsResponse0) = read_response(&mut Cursor::new(buff));
                debug!("versions: {:?}", versions);
                ()
            },
        );
    }
}
