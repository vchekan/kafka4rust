extern crate kafka4rust;
use kafka4rust::Producer;
use rand;
use rand::Rng;
use rand::distributions::Alphanumeric;
use failure::Error;
use log::debug;

fn random_topic() -> String{
    let mut topic = String::new();
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
        let mut producer: Producer<&str> = Producer::connect(bootstrap).await?;
        producer.send("msg1", &topic).await?;
        debug!("flushing");
        producer.flush().await;
        Ok::<(),Error>(())
    })?;

    Ok(())
}