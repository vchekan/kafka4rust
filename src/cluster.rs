use std::sync::{Arc, Mutex};

/// Produce Request:
///     resolve topic metadata (if not already),
///     open connection (if not already)
///     send and receive response, if broker failed, mark broker as failed and send message to
///     `self.recovery_loop(broker)`
#[derive(Debug)]
pub(crate) struct Cluster {
    bootstrap: Vec<String>,
    topic_meta: Arc<Mutex<Meta>>
}

#[derive(Debug)]
pub(crate) struct Meta {
    //topic_meta: HashMap<i32, protocol::TopicMetadata>

}


impl Cluster {
    pub fn new(bootstrap: Vec<String>) -> Self {
        Cluster { bootstrap, topic_meta: Arc::new(Mutex::new(Meta {})) }
    }

    pub fn produce_request<R>(&self, request: R, broker_id: i32) {
        let meta = self.topic_meta.lock().unwrap();
        //meta.topic_meta.get(brokerId).
    }

    /*
    pub fn connect(bootstrap: &Vec<&str>) -> impl Future<Item=Self, Error=String> {
        // copy topics
        let topics = topics.iter().map(|s| {s.to_string()}).collect();

        let bootstraps = bootstrap.iter().
            map(|addr|{ Broker::connect(&addr) });


        select_ok(bootstraps).
        // TODO: move it to BrokerConnection
        /*and_then(|(broker,_)| {
            debug!("Connected to {:?}", broker);
            let request = protocol::MetadataRequest0{topics};
            broker.request(&request)*/
        //map(move |(broker, response)| {
        map(|(broker,_)| {
            //debug!("Metadata response: {:?}", response);
            Cluster { brokers: response.brokers }
        })
    }
    */


}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    use futures::future::Future;
    use simplelog::*;

    #[test]
    fn resolve() {
        CombinedLogger::init(vec![TermLogger::new(LevelFilter::Debug, Config::default()).unwrap()]);
        debug!("Starting test");
        let cluster = Cluster::new(vec!["127.0.0.1:9092".to_string()]);
            /*map(|cluster| {
                info!("Bootstrapped: {:?}", cluster);
            }).
            map_err(|e| {
                error!("Bootstrap failed: {}", e);
            });*/

        //tokio::run(cluster);
    }
}