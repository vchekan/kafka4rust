use crate::connection::BrokerConnection;
use crate::protocol;
use crate::protocol::*;
use crate::error::{Result, BrokerFailureSource, InternalError};
use bytes::BytesMut;
use log::{debug, trace};
use std::fmt::{Debug, Formatter};
use std::io::Cursor;
use std::net::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing_attributes::instrument;
use futures::TryFutureExt;

//
// // TODO: if move negotiated api and correlation to broker connection, this struct degenerates.
// // Is it redundant?
// #[derive(Clone)]
// pub(crate) struct Broker {
//     //negotiated_api_version: Vec<(i16, i16)>, // TODO: just in case, make it property of
//     // connection, to renegotiate every time we connect.
//     //correlation_id: u32,    // TODO: is correlation property of broker or rather connection?
//     //correlation_id: AtomicUsize,
//     conn: BrokerConnection,
// }
//
// impl Debug for Broker {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("Broker")
//             .field("conn", &self.conn)
//             .finish()
//     }
// }

// impl Broker {
//     /// Connect to address and issue ApiVersion request, build compatible Api Versions for all Api
//     /// Keys
//     pub async fn connect(addr: SocketAddr) -> Result<Self> {
//         let conn = BrokerConnection::connect(addr)
//             .map_err(|e| InternalError::BrokerFailure(e))
//             .await?;
//         let req = protocol::ApiVersionsRequest0 {};
//         //let mut buf = Vec::with_capacity(1024);
//         let mut buf = BytesMut::with_capacity(1024);
//         // TODO: This is special case, we need correlationId and clientId before broker is created...
//         //let correlation_id = 0;
//
//         write_request(&req, None, &mut buf);
//         trace!("Requesting Api versions");
//         conn.request(&mut buf).await.map_err(|e| InternalError::BrokerFailure(e))?;
//
//         let mut cursor = Cursor::new(buf);
//         let (_corr_id, response): (u32, Result<protocol::ApiVersionsResponse0>) = read_response(&mut cursor);
//         let response = response?;
//         trace!("Got ApiVersionResponse {:?}", response);
//         response.error_code.as_result()?;
//         let negotiated_api_version = Broker::build_api_compatibility(&response);
//         Ok(Broker {
//             negotiated_api_version,
//             //correlation_id: AtomicUsize::new(1),
//             conn,
//         })
//     }
//
//     #[instrument(level = "debug", err, skip(self, request))]
//     pub async fn send_request<R>(&self, request: &R) -> Result<R::Response>
//     where
//         R: protocol::Request,
//     {
//         // TODO: buffer management
//         // TODO: ensure capacity (BytesMut will panic if out of range)
//         let mut buff = BytesMut::with_capacity(20 * 1024); //Vec::with_capacity(1024);
//         //let correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst) as u32;
//         // let correlation_id = CORRELATION_ID.fetch_add(1, Ordering::SeqCst) as u32;
//         protocol::write_request(request, None, &mut buff);
//
//         self.conn
//             .request(&mut buff)
//             .await
//             .map_err(|e| InternalError::BrokerFailure(e))?;
//         let mut cursor = Cursor::new(buff);
//         let (_corr_id, response): (_, Result<R::Response>) = read_response(&mut cursor);
//         let response = response?;
//         // TODO: check correlationId
//         // TODO: check for response error
//         Ok(response)
//     }
//
//     #[instrument(level = "debug", skip(self, request))]
//     pub async fn send_request2<R: FromKafka + Debug>(&self, mut request: BytesMut) -> Result<R> {
//         debug!("Sending conn.request()...");
//         self.conn
//             .request(&mut request)
//             .await
//             .map_err(|e| InternalError::BrokerFailure(e))?;
//         debug!("Sent conn.request(). Result buff len: {}", request.len());
//         let mut cursor = Cursor::new(request);
//         let (_corr_id, response): (_, Result<R>) = read_response(&mut cursor);
//         // TODO: check correlationId
//         // TODO: check for response error
//         response
//     }
//
//     /// Generate correlation_id and serialize request to buffer
//     pub fn mk_request<R>(&self, request: R) -> BytesMut
//     where
//         R: protocol::Request,
//     {
//         // TODO: buffer management
//         // TODO: dynamic buffer allocation
//         let mut buff = BytesMut::with_capacity(10 * 1024); //Vec::with_capacity(1024);
//         protocol::write_request(&request, None, &mut buff);
//
//         buff
//     }
//
//     fn build_api_compatibility(them: &protocol::ApiVersionsResponse0) -> Vec<(i16, i16)> {
//         //
//         // them:  mn----mx
//         // me  :             mn-------mx
//         // join:        mx < mn
//         //
//         // Empty join: max<min. For successful join: min<=max
//         //
//         let my_versions = protocol::supported_versions();
//         trace!(
//             "build_api_compatibility my_versions: {:?} them: {:?}",
//             my_versions,
//             them
//         );
//
//         them.api_versions
//             .iter()
//             .map(
//                 |them| match my_versions.iter().find(|(k, _, _)| them.api_key == *k) {
//                     Some((k, mn, mx)) => {
//                         let agreed_min = mn.max(&them.min_version);
//                         let agreed_max = mx.min(&them.max_version);
//                         if agreed_min <= agreed_max {
//                             Some((*k, *agreed_max))
//                         } else {
//                             None
//                         }
//                     }
//                     None => None,
//                 },
//             )
//             .flatten()
//             .collect()
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use async_std::task;
//     use log::debug;
//     use std::env;
//
//     #[test]
//     fn negotiate_api_works() {
//         simple_logger::init_with_level(log::Level::Debug).unwrap();
//
//         let bootstrap = env::var("kafka-bootstrap").unwrap_or("127.0.0.1:9092".to_string());
//         let addr: SocketAddr = bootstrap
//             .to_socket_addrs()
//             .unwrap()
//             .next()
//             .expect(format!("Host '{}' not found", bootstrap).as_str());
//
//         task::block_on(async {
//             let broker = super::Broker::connect(addr).await.unwrap();
//             info!("Connected: {:?}", broker);
//
//             let req = MetadataRequest0 {
//                 topics: vec!["test".into()],
//             };
//             let meta = broker.send_request(&req).await.unwrap();
//             debug!("Meta response: {:?}", meta);
//         });
//     }
// }
