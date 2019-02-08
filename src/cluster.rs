use crate::broker::Broker;
use futures::{future, stream::StreamExt};
use std::io;

/// Produce Request:
///     resolve topic metadata (if not already),
///     open connection (if not already)
///     send and receive response, if broker failed, mark broker as failed and send message to
///     `self.recovery_loop(broker)`
#[derive(Debug)]
pub(crate) struct Cluster {
    bootstrap: Vec<String>,
    //topic_meta: Arc<Mutex<Meta>>,
}

#[derive(Debug)]
pub(crate) struct Meta {
    //topic_meta: HashMap<i32, protocol::TopicMetadata>
}

impl Cluster {
    pub fn new(bootstrap: Vec<String>) -> Self {
        Cluster {
            bootstrap,
            //topic_meta: Arc::new(Mutex::new(Meta {})),
        }
    }

    /*pub fn produce_request<R>(&self, _request: R, _broker_id: i32) {
        let _meta = self.topic_meta.lock().unwrap();
        //meta.topic_meta.get(brokerId).
    }*/

    pub async fn connect<'a>(bootstrap: &'a Vec<&'a str>) -> io::Result<Self> {
        let connect_futures = bootstrap
            .iter()
            .filter_map(|addr| addr.parse().ok())
            .map(|addr| Broker::connect(addr));

        // TODO: use `fold` to preserve and report last error if no result
        /*let resolved = futures::stream::futures_unordered(connectFutures).
                filter_map(|f| future::ready(f.ok())).
                next();
        let broker = match await!(resolved) {
            Some(broker) => broker,
            // TODO: failure
            None => return Err(io::Error::from(io::ErrorKind::NotFound)),
        };*/

        let mut resolved1 = futures::stream::futures_unordered(connect_futures)
            .filter_map(|f| future::ready(f.ok()));
        let broker = match await!(resolved1.next()) {
            Some(broker) => broker,
            // TODO: failure
            None => return Err(io::Error::from(io::ErrorKind::NotFound)),
        };

        // TODO: move it to BrokerConnection
        debug!("Connected to {:?}", broker);

        /*let request = protocol::MetadataRequest0{topics};
        let response = await!(broker.request(&request));
        debug!("Metadata response: {:?}", response);
        */

        let bootstrap: Vec<String> = bootstrap.iter().map(|&a| a.to_string()).collect();
        Ok(Cluster {
            bootstrap,
            //topic_meta: response
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor;
    use simplelog::*;

    #[test]
    fn resolve() {
        CombinedLogger::init(vec![
            TermLogger::new(LevelFilter::Debug, Config::default()).unwrap()
        ])
        .unwrap();
        debug!("Starting test");
        executor::block_on(
            async {
                let addr = vec!["127.0.0.1:9092"];
                let cluster = await!(Cluster::connect(&addr));
                info!("Bootstrapped: {:?}", cluster);
            },
        );
    }
}
