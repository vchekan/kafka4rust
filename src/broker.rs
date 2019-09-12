use crate::connection::BrokerConnection;
use crate::protocol;
use crate::protocol::*;
use std::io::{self, Cursor};
use std::net::*;

// TODO: if move negotiated api and correlation to broker connection, this struct degenerates.
// Is it redundant?
#[derive(Debug)]
pub(crate) struct Broker {
    /// (api_key, agreed_version)
    negotiated_api_version: Vec<(i16, i16)>,    // TODO: just in case, make it property of
    // connection, to renegotiate every time we connect.
    correlation_id: u32,    // TODO: is correlation property of broker or rather connection?
    conn: BrokerConnection,
}

#[derive(Debug, Fail)]
pub enum BrokerError {
    #[fail(display = "invalid broker address: '{}'", address)]
    InvalidBrokerAddress { address: String },
}

impl Broker {
    /// Connect to address and issue ApiVersion request, build compatible Api Versions for all Api
    /// Keys
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let mut conn = BrokerConnection::connect(addr).await?;
        let req = protocol::ApiVersionsRequest0 {};
        let mut buf = Vec::with_capacity(1024);
        // TODO: This is special case, we need correlationId and clientId before broker is created...
        let correlation_id = 0;
        let client_id = None;

        write_request(&req, correlation_id, client_id, &mut buf);
        debug!("Connected to {:?}, Requesting {:?}", conn, req);
        conn.request(&mut buf).await?;

        let mut cursor = Cursor::new(buf);
        let (_corr_id, response) = read_response(&mut cursor);
        debug!("Got ApiVersionResponse {:?}", response);
        let negotiated_api_version = Broker::get_api_compatibility(&response);
        Ok(Broker {
            negotiated_api_version,
            correlation_id: 1,
            conn,
        })
    }

    pub async fn request<'a, R>(&'a self, request: &'a R) -> io::Result<R::Response>
    where
        R: protocol::Request,
    {
        // TODO: buffer management
        let mut buff = Vec::with_capacity(1024);
        protocol::write_request(request, self.correlation_id, None, &mut buff);
        self.correlation_id += 1;

        self.conn.request(&mut buff).await?;
        let mut cursor = Cursor::new(buff);
        let (corr_id, response) : (_, R::Response) = read_response(&mut cursor);
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
        let bootstrap = env::var("kafka-bootstrap").unwrap_or("127.0.0.1:9092".to_string());
        let addr: SocketAddr = bootstrap.to_socket_addrs().unwrap().next().expect(format!("Host '{}' not found", bootstrap).as_str());

        executor::block_on(
            async {
                let broker = super::Broker::connect(addr).await.unwrap();
                println!("Broker: {:?}", broker);
            },
        );
    }
}
