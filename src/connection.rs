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

use std::time::Duration;
use bytes::{BytesMut, BufMut, Bytes, Buf};
use std::net::SocketAddr;

use crate::error::{BrokerFailureSource, BrokerResult};
use async_std::net::TcpStream;
use async_std::prelude::*;
use tracing_attributes::instrument;
use tracing_futures::Instrument;
use std::fmt::{Debug, Formatter};
use crate::protocol;
use crate::protocol::{write_request, read_response};
use tokio::sync::{mpsc, oneshot};
use log::{debug, trace, info};
use crate::utils::TracedMessage;

pub(crate) const CLIENT_ID: &str = "k4rs";

pub(crate) struct BrokerConnection {
    addr: SocketAddr,
    /// (api_key, agreed_version)
    negotiated_api_version: Vec<(i16, i16)>,
    tcp: TcpStream,
    correlation_id: u32,
}

impl BrokerConnection {
    /// Connect to address and issue ApiVersion request, build compatible Api Versions for all Api
    /// Keys
    #[instrument(level="debug")]
    pub async fn connect(addr: SocketAddr) -> BrokerResult<Self> {
        let tcp = TcpStream::connect(&addr).await?;
        debug!("Connected to {}", addr);
        let mut conn = BrokerConnection { addr, negotiated_api_version: vec![], tcp, correlation_id: 0};
        let req = protocol::ApiVersionsRequest0 {};
        //let mut buf = Vec::with_capacity(1024);
        let mut buf = BytesMut::with_capacity(1024);
        // TODO: This is special case, we need correlationId and clientId before broker is created...
        let correlation_id = 0;
        write_request(&req, None, &mut buf, correlation_id);
        trace!("Requesting Api versions");
        conn.exchange_with_buf(&mut buf).await?;

        let mut buf = buf.freeze();
        let (corr_id, response): (u32, protocol::ApiVersionsResponse0) = read_response(&mut buf)?;
        debug!("Got ApiVersionResponse {:?}; correlation_id: {}, buf: {:?}", response, corr_id, buf);
        response.error_code.as_result()?;
        let negotiated_api_version = build_api_compatibility(&response);

        Ok(BrokerConnection {
            addr: conn.addr,
            negotiated_api_version,
            tcp: conn.tcp,
            correlation_id: 1,
        })
    }

    pub fn addr(&self) -> SocketAddr {self.addr}


    /// Write request from buffer into tcp and reuse the buffer to read response.
    /// Message size is read from the buffer, so buffer will position to the correlation_id
    #[instrument(level = "debug", err, skip(self, buf))]
    pub async fn exchange_with_buf(&mut self, buf: &mut BytesMut) -> BrokerResult<()> {
        trace!("Sending request[{}] to {:?}", buf.len(), self.tcp.peer_addr());
        self.tcp.write_all(buf).instrument(tracing::debug_span!("writing request")).await
            .map_err(|e| BrokerFailureSource::Write(format!("writing {} bytes to socket {:?}", buf.len(), self.tcp.peer_addr()), e))?;

        // TODO: buffer reuse
        buf.clear();
        // Read length into buffer
        buf.resize(4, 0_u8);
        // TODO: ensure length is sane
        self.tcp.read_exact(buf).instrument(tracing::info_span!("reading msg len")).await
            .map_err(|e| BrokerFailureSource::Read(buf.len(), e))?;
        let len = buf.get_u32();
        //debug!("Response len: {}, reading body...", len);
        buf.resize(len as usize, 0_u8);
        self.tcp.read_exact(buf).instrument(tracing::info_span!("reading msg body")).await
            .map_err(|e| BrokerFailureSource::Read(buf.len(), e))?;

        // TODO: validate correlation_id
        // TODO: why little endian???
        // let correlation_id = buf.get_u32(); //byteorder::LittleEndian::read_u32(&buf);
        // debug!("Read correlation_id: {}", correlation_id);
        Ok(())
    }

    #[instrument(level = "debug", err, skip(self, request))]
    pub async fn exchange<R>(&mut self, request: &R) -> BrokerResult<R::Response>
    where
        R: protocol::Request,
    {
        // TODO: buffer management
        // TODO: ensure capacity (BytesMut will panic if out of range)
        let mut buff = BytesMut::with_capacity(20 * 1024); //Vec::with_capacity(1024);
        //let correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst) as u32;
        // let correlation_id = CORRELATION_ID.fetch_add(1, Ordering::SeqCst) as u32;
        protocol::write_request(request, None, &mut buff, self.correlation_id);
        self.correlation_id = self.correlation_id.wrapping_add(1);

        self.exchange_with_buf(&mut buff).await?;
        //let mut cursor = Cursor::new(buff);
        let (_corr_id, response) = read_response(&mut buff.freeze())?;
        // TODO: check correlationId
        // TODO: check for response error
        Ok(response)
    }

    #[instrument(level="debug")]
    pub async fn fetch_topic_with_broker(&mut self, topics: Vec<String>, timeout: Duration) -> BrokerResult<protocol::MetadataResponse0> {
        let req = protocol::MetadataRequest0 {
            topics,
        };

        self.exchange(&req).await
    }


}

fn build_api_compatibility(them: &protocol::ApiVersionsResponse0) -> Vec<(i16, i16)> {
    //
    // them:  mn----mx
    // me  :             mn-------mx
    // join:        mx < mn
    //
    // Empty join: max<min. For successful join: min<=max
    //
    let my_versions = protocol::supported_versions();
    trace!(
        "build_api_compatibility my_versions: {:?} them: {:?}",
        my_versions,
        them
    );

    them.api_versions
        .iter()
        .map(
            |them| match my_versions.iter().find(|(k, _, _)| them.api_key == *k) {
                Some((k, mn, mx)) => {
                    let agreed_min = mn.max(&them.min_version);
                    let agreed_max = mx.min(&them.max_version);
                    if agreed_min <= agreed_max {
                        Some((*k, *agreed_max))
                    } else {
                        None
                    }
                }
                None => None,
            },
        )
        .flatten()
        .collect()
}

// #[instrument(name="connection-handler")]
// async fn run(addr: SocketAddr, rx: mpsc::Receiver<TracedMessage<Msg>>) {
//     let conn = BrokerConnection::connect(addr).await;
//     match conn {
//         Ok(mut conn) => {
//             debug!("run: connected, starting message loop");
//             while let Some(msg) = conn.rx.recv().await {
//                 conn.handle(msg).await;
//             }
//         }
//         Err(e) => {
//             info!("Failed to connect broker: {}", e);
//         }
//     }
// }

// TODO: try to show local socket info too
impl Debug for BrokerConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerConnection")
            .field("addr", &self.addr)
            .finish()
    }
}

// impl Debug for ConnectionHandle {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
//         f.debug_struct("ConnectionHandle").field("addr", &self.addr).finish()
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        read_response, write_request, ApiVersionsRequest0, ApiVersionsResponse0,
    };
    use std::env;
    use std::net::ToSocketAddrs;

    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        simple_logger::SimpleLogger::new().env().with_level(log::LevelFilter::Debug).init().unwrap();

        let bootstrap = env::var("kafka-bootstrap").unwrap_or("127.0.0.1:9092".to_string());
        let addr = bootstrap
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect(format!("Host '{}' not found", bootstrap).as_str());

        let mut conn = BrokerConnection::connect(addr).await?;
        let meta = conn.fetch_topic_with_broker(vec!["test1".to_string()], Duration::from_secs(10)).await?;
        println!("Meta: {:?}", meta);

        Ok(())
    }
}
