use tokio::prelude::*;
use cluster::Cluster;
use futures::stream;
use protocol;

#[derive(Default, Builder)]
#[builder(setter(into), default)]
pub struct ConsumerConfig {
    #[builder(default = "\"localhost\".to_string()")]
    pub bootstrap: String,
    pub topic: String
}

#[derive(Debug)]
pub struct Message {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct Consumer {
    config: ConsumerConfig,
    cluster: Cluster,
}

impl Consumer {
    pub fn new(bootstrap: &str, topic: &str) -> Consumer {
        let config = ConsumerConfigBuilder::default().
            bootstrap(bootstrap).
            // TODO: how to make topic mandatory?
            topic(topic).
            build().unwrap();

        let cluster = Cluster::new(vec![bootstrap.to_string()]);

        Consumer {config, cluster}
    }

    pub fn on_message(mut self) -> impl Stream<Item=Vec<Message>, Error=()> {
        /*stream::poll_fn(|| -> Poll<Option<Vec<Message>>, io::Error> {
            if self.
        });*/
        stream::unfold(self, |consumer| {
            let msg = vec![Message{key: vec![], value: vec![]}];
            let fut = future::ok((msg, consumer));
            Some(fut)
        })
    }

    // TODO: make Error something more meaningful (fault::)
    pub fn consume(mut self, topics: &[&str]) -> impl Future<Item=protocol::MetadataResponse0, Error=String> {
                                                    //Stream<Item=Vec<Message>, Error=String> {
        //stream::empty()
        // TODO: can reuse bootstrap connection?
        self.cluster.bootstrap(topics).
            map(|meta| {
                meta//.topics
            })
    }

    fn connect_brokers(meta: protocol::MetadataResponse0) {
        // map every broker into a stream
        //meta.topic_metadata
    }

    fn connect_broker(broker: &protocol::Broker) {
        //let addr =
        //let addr = SocketAddr::new(addr, broker.port)
        //BrokerConnection::new(addr)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio;

    #[test]
    fn test() {
        info!("info!");
        let consumer = Consumer::new("localhost", "test1");

        /*let msgs = consumer.on_message().take(3).
            for_each(|m| {
                println!("message: {:?}", m);
                future::ok(())
            });
            */
        let msgs = consumer.consume(&vec!["test1"]).
            map(|x| {
                println!("consume: {:?}", x)
            }).map_err(|e| {println!("Error: {:?}", e)});

        tokio::run(msgs);
    }
}