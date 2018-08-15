use tokio::prelude::*;
use cluster::Cluster;
use std::io;

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

pub struct ConnectStateFuture {

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

    pub fn connect(&mut self) -> impl Stream<Item=ConnectStateFuture, Error=()> {
        debug!("Connecting");
        stream::empty()
    }

    pub fn close(&mut self) -> impl Stream<Item=ConnectStateFuture, Error=()> {
        stream::empty()
    }

    pub fn on_connect(&mut self) -> impl Stream<Item=ConnectStateFuture, Error=()> {
        stream::empty()
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
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio;

    #[test]
    fn test() {
        info!("info!");
        let consumer = Consumer::new("localhost", "test1");

        let msgs = consumer.on_message().take(3).
            for_each(|m| {
                println!("message: {:?}", m);
                future::ok(())
            });

        tokio::run(msgs);
    }
}