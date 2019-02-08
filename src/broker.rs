use crate::connection::BrokerConnection;
use crate::protocol;
use crate::protocol::*;
use std::io::{self, Cursor};
use std::net::*;

#[derive(Debug)]
pub(crate) struct Broker {
    /// (api_key, agreed_version)
    negotiated_api_version: Vec<(i16, i16)>,
    correlation_id: u32,
    conn: BrokerConnection,
}

#[derive(Debug, Fail)]
pub enum BrokerError {
    #[fail(display = "invalid broker address: '{}'", address)]
    InvalidBrokerAddress { address: String },
}

impl Broker {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let mut conn = await!(BrokerConnection::connect(addr))?;
        let req = protocol::ApiVersionsRequest0 {};
        let mut buf = Vec::with_capacity(1024);
        // TODO: This is special case, we need correlationId and clientId before broker is created...
        let correlation_id = 0;
        let client_id = None;

        write_request(&req, correlation_id, client_id, &mut buf);
        debug!("Connected to {:?}, Requesting {:?}", conn, req);
        await!(conn.request(&mut buf))?;

        let mut cursor = Cursor::new(buf);
        let (_corr_id, response) = read_response::<protocol::ApiVersionsResponse0>(&mut cursor);
        debug!("Got ApiVersionResponse {:?}", response);
        let negotiated_api_version = Broker::get_api_compatibility(&response);
        Ok(Broker {
            negotiated_api_version,
            correlation_id: 1,
            conn,
        })
    }

    pub async fn request<'a, R>(&'a mut self, request: &'a R) -> io::Result<R::Response>
    where
        R: protocol::Request,
    {
        // TODO: buffer management
        let mut buff = Vec::with_capacity(1024);
        protocol::write_request(request, self.correlation_id, None, &mut buff);
        self.correlation_id += 1;

        await!(self.conn.request(&mut buff))?;
        let mut cursor = Cursor::new(buff);
        let (corr_id, response) = read_response::<R::Response>(&mut cursor);
        // TODO: check correlationId
        // TODO: check for response error
        debug!("CorrId: {}, Response: {:?}", corr_id, response);
        Ok(response)
    }

    fn get_api_compatibility(them: &protocol::ApiVersionsResponse0) -> Vec<(i16, i16)> {
        //
        // them:  mn----mx
        // me  :             mn-------mx
        // join:        mx < mn
        //
        // Empty join: max<min. For successful join: min<=max
        //
        let my_versions = protocol::supported_versions();
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
    use futures::executor;
    use std::env;

    #[test]
    fn negotiate_api_works() {
        let bootstrap = env::var("kafka-bootstrap").unwrap_or("127.0.0.1".to_string());
        let addr = format!("{}:9092", bootstrap);
        let addr: SocketAddr = addr.parse().unwrap();

        executor::block_on(
            async {
                let broker = await!(super::Broker::connect(addr)).unwrap();
                println!("Broker: {:?}", broker);
            },
        );
    }
}
