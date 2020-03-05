extern crate kafka4rust;
use kafka4rust::{Producer, Consumer};
use rand;
use rand::Rng;
use rand::distributions::Alphanumeric;
use failure::Error;
use std::collections::HashSet;

fn random_topic() -> String{
    let topic: String = rand::thread_rng().sample_iter(Alphanumeric).take(7).collect();
    format!("test_{}", topic)
}

#[test]
fn topic_is_autocreated_by_producer() -> Result<(),Error> {
    let mut runtime = tokio::runtime::Builder::new().
        basic_scheduler().
        core_threads(2).
        thread_name("test_k4rs").build()?;
    simple_logger::init_with_level(log::Level::Debug)?;

    runtime.block_on(async {
        let bootstrap = "localhost";
        let topic = random_topic();
        let count = 200;
        let mut sent = HashSet::new();
        let mut producer= Producer::connect(bootstrap).await?;
        
        for i in 1..=count {
            let msg = format!("m{}", i);
            sent.insert(msg.clone());
            producer.send(msg, &topic).await?;
        }
        producer.flush().await;

        let mut consumer = Consumer::builder().
            topic(topic).
            bootstrap("localhost").build().await?;
        for i in 1..=count {
            let msg = consumer.recv().await.unwrap();
            let msg = String::from_utf8(msg.value).unwrap();
            assert!(sent.remove(&msg));
        }
        assert!(sent.is_empty());

        Ok::<(),Error>(())
    })?;

    Ok(())
}