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

use crate::error::{BrokerFailureSource, InternalError, BrokerResult};
use async_std::net::TcpStream;
use async_std::prelude::*;
use std::sync::Arc;
use tracing_attributes::instrument;
use tracing_futures::Instrument;
use tracing;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicUsize;
use crate::protocol;
use crate::protocol::{write_request, read_response};
use std::io::Cursor;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use futures::TryFutureExt;
use crate::protocol::FromKafka;

// TODO: deal with u32 overflow
pub static CORRELATION_ID: AtomicUsize = AtomicUsize::new(0);

pub(crate) const CLIENT_ID: &str = "k4rs";

//#[derive(Clone)]
pub(crate) struct BrokerConnection {
    addr: SocketAddr,
    /// (api_key, agreed_version)
    negotiated_api_version: Vec<(i16, i16)>,
    // TODO: is this decision sound? Could we write 2 messages from 2 threads and read them out of order?
    //inner: Arc<Mutex<Inner>>,
    tcp: Mutex<TcpStream>,
}

// TODO: try to show local socket info too
impl Debug for BrokerConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerConnection")
            .field("addr", &self.addr)
            .finish()
    }
}

impl BrokerConnection {
    /// Connect to address and issue ApiVersion request, build compatible Api Versions for all Api
    /// Keys
    pub async fn connect(addr: SocketAddr) -> BrokerResult<Self> {
        let tcp = TcpStream::connect(&addr).await?;
        debug!("Connected to {}", addr);
        let conn = BrokerConnection { addr, negotiated_api_version: vec![], tcp: Mutex::new(tcp)};
        //let conn = BrokerConnection::connect(addr)
            //.map_err(|e| InternalError::BrokerFailure(e))
            // .await?;
        let req = protocol::ApiVersionsRequest0 {};
        //let mut buf = Vec::with_capacity(1024);
        let mut buf = BytesMut::with_capacity(1024);
        // TODO: This is special case, we need correlationId and clientId before broker is created...
        //let correlation_id = 0;

        write_request(&req, None, &mut buf);
        trace!("Requesting Api versions");
        conn.request(&mut buf).await?;

        let mut cursor = Cursor::new(buf);
        let (_corr_id, response): (u32, protocol::ApiVersionsResponse0) = read_response(&mut cursor)?;
        debug!("Got ApiVersionResponse {:?}", response);
        response.error_code.as_result()?;
        let negotiated_api_version = build_api_compatibility(&response);

        Ok(BrokerConnection{
            addr: conn.addr,
            negotiated_api_version,
            tcp: conn.tcp
        })
    }

    // /// Connect to address but do not perform any check beyond successful tcp connection.
    // pub async fn connect(addr: SocketAddr) -> Result<Self> {
    //     let tcp = TcpStream::connect(&addr).await?;
    //     let conn = BrokerConnection { addr, negotiated_api_version: vec![], tcp, };
    //     Ok(conn)
    // }

    /// Write request from buffer into tcp and reuse the buffer to read response
    #[instrument(level = "debug", err, skip(self, buf))]
    pub async fn request(&self, buf: &mut BytesMut) -> BrokerResult<()> {
        let mut tcp = self.tcp.lock().await;
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

    #[instrument(level = "debug", err, skip(self, request))]
    pub async fn send_request<R>(&self, request: &R) -> BrokerResult<R::Response>
    where
        R: protocol::Request,
    {
        // TODO: buffer management
        // TODO: ensure capacity (BytesMut will panic if out of range)
        let mut buff = BytesMut::with_capacity(20 * 1024); //Vec::with_capacity(1024);
        //let correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst) as u32;
        // let correlation_id = CORRELATION_ID.fetch_add(1, Ordering::SeqCst) as u32;
        protocol::write_request(request, None, &mut buff);

        self.request(&mut buff).await?;
        let mut cursor = Cursor::new(buff);
        let (_corr_id, response) = read_response(&mut cursor)?;
        // TODO: check correlationId
        // TODO: check for response error
        Ok(response)
    }

    /// Generate correlation_id and serialize request to buffer
    /// TODO: it does not make sense to have this function in connection class
    pub fn mk_request<R>(&self, request: R) -> BytesMut
    where
        R: protocol::Request,
    {
        // TODO: buffer management
        // TODO: dynamic buffer allocation
        let mut buff = BytesMut::with_capacity(10 * 1024); //Vec::with_capacity(1024);
        protocol::write_request(&request, None, &mut buff);

        buff
    }

    #[instrument(level = "debug", skip(self, request))]
    pub async fn send_request2<R: FromKafka + Debug>(&self, request: &mut BytesMut) -> BrokerResult<R> {
        debug!("Sending conn.request()...");
        self.request(request).await?;
        debug!("Sent conn.request(). Result buff len: {}", request.len());
        let mut cursor = Cursor::new(request);
        let (_corr_id, response): (_, R) = read_response(&mut cursor)?;
        // TODO: check correlationId
        // TODO: check for response error
        Ok(response)
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
