use crate::connection::BrokerConnection;
use crate::error::Result;
use crate::protocol;
use crate::protocol::*;
use failure::_core::fmt::Debug;
use log::{debug, trace};
use std::io::Cursor;
use std::net::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use bytes::BytesMut;
use failure::ResultExt;

// TODO: if move negotiated api and correlation to broker connection, this struct degenerates.
// Is it redundant?
#[derive(Debug)]
pub(crate) struct Broker {
    /// (api_key, agreed_version)
    negotiated_api_version: Vec<(i16, i16)>, // TODO: just in case, make it property of
    // connection, to renegotiate every time we connect.
    //correlation_id: u32,    // TODO: is correlation property of broker or rather connection?
    correlation_id: AtomicUsize,
    conn: BrokerConnection,
}

impl Broker {
    /// Connect to address and issue ApiVersion request, build compatible Api Versions for all Api
    /// Keys
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let conn = BrokerConnection::connect(addr).await.context("Broker:connect")?;
        let req = protocol::ApiVersionsRequest0 {};
        //let mut buf = Vec::with_capacity(1024);
        let mut buf = BytesMut::with_capacity(1024);
        // TODO: This is special case, we need correlationId and clientId before broker is created...
        let correlation_id = 0;

        write_request(&req, correlation_id, None, &mut buf);
        debug!("Requesting Api versions");
        conn.request(&mut buf).await.context("Broker:connect:request")?;

        let mut cursor = Cursor::new(buf);
        let (_corr_id, response) = read_response(&mut cursor);
        trace!("Got ApiVersionResponse {:?}", response);
        let negotiated_api_version = Broker::build_api_compatibility(&response);
        Ok(Broker {
            negotiated_api_version,
            correlation_id: AtomicUsize::new(1),
            conn,
        })
    }

    pub async fn send_request<R>(&self, request: R) -> Result<R::Response>
    where
        R: protocol::Request,
    {
        // TODO: buffer management
        // TODO: ensure capacity (BytesMut will panic if out of range)
        let mut buff = BytesMut::with_capacity(1024); //Vec::with_capacity(1024);
        let correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst) as u32;
        protocol::write_request(&request, correlation_id, None, &mut buff);

        self.conn.request(&mut buff).await.context("Broker: sending request")?;
        let mut cursor = Cursor::new(buff);
        let (corr_id, response): (_, R::Response) = read_response(&mut cursor);
        // TODO: check correlationId
        // TODO: check for response error
        Ok(response)
    }

    pub async fn send_request2<R: FromKafka + Debug>(&self, mut request: BytesMut) -> Result<R>
    {
        debug!("Sending conn.request()...");
        self.conn.request(&mut request).await.context("Broker: sending request")?;
        debug!("Sent conn.request(). Result buff len: {}", request.len());
        let mut cursor = Cursor::new(request);
        let (corr_id, response): (_, R) = read_response(&mut cursor);
        // TODO: check correlationId
        // TODO: check for response error
        Ok(response)
    }

    pub fn mk_request<R>(&self, request: R) -> BytesMut
        where
            R: protocol::Request,
    {
        // TODO: buffer management
        let mut buff = BytesMut::with_capacity(1024); //Vec::with_capacity(1024);
        let correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst) as u32;
        protocol::write_request(&request, correlation_id, None, &mut buff);

        buff
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
            my_versions, them
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
}

#[cfg(test)]
mod tests {
    use super::*;
    //use futures::executor;
    use async_std::task;
    use log::debug;
    use std::env;

    #[test]
    fn negotiate_api_works() {
        simple_logger::init_with_level(log::Level::Debug).unwrap();

        let bootstrap = env::var("kafka-bootstrap").unwrap_or("127.0.0.1:9092".to_string());
        let addr: SocketAddr = bootstrap
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect(format!("Host '{}' not found", bootstrap).as_str());

        task::block_on(async {
            let broker = super::Broker::connect(addr).await.unwrap();
            info!("Connected: {:?}", broker);

            let req = MetadataRequest0 {
                topics: vec!["test".into()],
            };
            let meta = broker.send_request(req).await.unwrap();
            debug!("Meta response: {:?}", meta);
        });
    }
}
