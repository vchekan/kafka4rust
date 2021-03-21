//! Connection should support pipelining requests. At the same time same connection should serve
//! multiple customers: recovery manager, metadata requestor, data sender. This means that same
//! connection reference should be possible to share (behind `Rc`). This implies that Connection's
//! functions should be immutable and appropriate operations locked. Despite being executed in
//! single-threaded executor, interleaving can occur in functions, such as `Tcp_Stream::write_all`
//! which would cause data corruption.
//! TODO: Q: does it guarantee that reads are in the same order as writes?
//!
//! Alternative design would be to lock Connection, write data and obtain read data future, release
//! lock and await fore read data outside of lock.
//!
//! Yet another alternative is to split TcpStream into writer and reader, but then some work is
//! needed to expose request-response as a single future. But WriteHalf does not have Clone, so it
//! is no go.
//!
//! Read loop design.
//! Read loop can set `current_response` but upon next iteration, how does it know that message has
//! been handled already? It seems like response handler driving the read is more natural.
//!
//! Write channel: how to implement sender's pushback?

use byteorder::BigEndian;
use bytes::{ByteOrder, BytesMut};
use std::net::SocketAddr;

use crate::error::{BrokerFailureSource};
use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::sync::Mutex;
use std::sync::Arc;
use tracing_attributes::instrument;
use tracing_futures::Instrument;
use tracing;
use std::fmt::{Debug, Formatter};

type Result<T> = crate::error::Result<T, BrokerFailureSource>;
pub(crate) const CLIENT_ID: &str = "k4rs";

pub(crate) struct BrokerConnection {
    addr: SocketAddr,
    // TODO: is this decision sound? Could we write 2 messages from 2 threads and read them out of order?
    inner: Arc<Mutex<Inner>>,
}

// TODO: try to show local socket info too
impl Debug for BrokerConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerConnection")
            .field("addr", &self.addr)
            .finish()
    }
}

#[derive(Debug)]
struct Inner {
    // TODO: move correlation here
    //correlation_id: u32,
    tcp: TcpStream,
}

impl BrokerConnection {
    /// Connect to address but do not perform any check beyond successful tcp connection.
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let tcp = TcpStream::connect(&addr).await?;

        let conn = BrokerConnection {
            addr,
            inner: Arc::new(Mutex::new(Inner {
                //correlation_id: 0,
                tcp,
            })),
        };

        Ok(conn)
    }

    /// Write request from buffer into tcp and reuse the buffer to read response
    #[instrument(level = "debug", err, skip(self, buf))]
    pub async fn request(&self, buf: &mut BytesMut) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let tcp = &mut inner.tcp;
        trace!("Sending request[{}] to {:?}", buf.len(), tcp.peer_addr());
        tcp.write_all(&buf).instrument(tracing::debug_span!("writing request")).await
            .map_err(|e| BrokerFailureSource::Write(format!("writing {} bytes to socket {:?}", buf.len(), tcp.peer_addr()), e))?;

        // TODO: buffer reuse
        buf.clear();
        // Read length into buffer
        buf.resize(4, 0_u8);
        // TODO: ensure length is sane
        tcp.read_exact(buf).instrument(tracing::info_span!("reading msg len")).await
            .map_err(|e| BrokerFailureSource::Read(buf.len(), e))?;
        let len = BigEndian::read_u32(&buf);
        //debug!("Response len: {}, reading body...", len);
        buf.resize(len as usize, 0_u8);
        tcp.read_exact(buf).instrument(tracing::info_span!("reading msg body")).await
            .map_err(|e| BrokerFailureSource::Read(buf.len(), e))?;

        // TODO: validate correlation_id
        let _correlation_id = byteorder::LittleEndian::read_u32(&buf);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        read_response, write_request, ApiVersionsRequest0, ApiVersionsResponse0,
    };
    use async_std::task;
    use std::env;
    use std::io::Cursor;
    use std::net::ToSocketAddrs;
    use std::sync::Arc;
    use crate::error::InternalError;

    #[test]
    fn it_works() {
        simple_logger::init_with_level(log::Level::Debug).unwrap();

        let bootstrap = env::var("kafka-bootstrap").unwrap_or("localhost:9092".to_string());
        let addr = bootstrap
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect(format!("Host '{}' not found", bootstrap).as_str());

        task::block_on(async move {
            let conn = Arc::new(BrokerConnection::connect(addr).await.unwrap());
            info!("conn: {:?}", conn);
            for _ in 0..2 {
                let conn = conn.clone();
                task::spawn(async move {
                    let request = ApiVersionsRequest0 {};
                    let mut buff = BytesMut::with_capacity(1024); //Vec::new();
                    write_request(&request, 0, None, &mut buff);
                    conn.request(&mut buff).await.unwrap();
                    let (correlation_id, versions): (_, std::result::Result<ApiVersionsResponse0,InternalError>) =
                        read_response(&mut Cursor::new(buff));
                    debug!(
                        "correlationId: {}, versions: {:?}",
                        correlation_id, versions
                    );
                });
            }
        });
    }
}
