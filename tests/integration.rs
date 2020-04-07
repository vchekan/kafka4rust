extern crate kafka4rust;
mod utils;

use kafka4rust::{Producer, Consumer};
use rand;
use rand::Rng;
use rand::distributions::Alphanumeric;
use failure::Error;
use std::collections::HashSet;
use tokio;
use crate::utils::*;
use log::debug;
use tracing::{self, Level, event, span, dispatcher};
use opentelemetry::{api::Provider, sdk, global, api::trace::tracer::TracerGenerics};
use opentelemetry::api::{Tracer, Span, KeyValue};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;
use tracing_futures::Instrument;
use std::time::Duration;

fn random_topic() -> String{
    let topic: String = rand::thread_rng().sample_iter(Alphanumeric).take(7).collect();
    format!("test_{}", topic)
}

#[tokio::test]
async fn topic_is_autocreated_by_producer() -> Result<(),Error> {
    simple_logger::init_with_level(log::Level::Trace)?;
    init_tracer()?;
    let tracer = global::trace_provider().get_tracer("component1");
    let otl = OpenTelemetryLayer::with_tracer(tracer);
    let subscriber = Registry::default().with(otl);
    let dispatch = dispatcher::Dispatch::new(subscriber);
    dispatcher::set_global_default(dispatch)?;

    let span = tracing::span!(tracing::Level::INFO, "topic_is_autocreated_by_producer");
    let _guard = span.enter();

    let bootstrap = "localhost";
    let topic = random_topic();
    let count = 200;
    let mut sent = HashSet::new();
    let mut producer = Producer::connect(bootstrap).await?;

    {
        let span = tracing::span!(tracing::Level::ERROR, "send");
        let _guard = span.enter();
        for i in 1..=count {
            let msg = format!("m{}", i);
            sent.insert(msg.clone());
            event!(Level::INFO, %msg, "Sent");
            producer.send(msg, &topic).await?;
        }
        producer.flush().await;
    }

    {
        let span = tracing::span!(tracing::Level::ERROR, "receive");
        let _guard = span.enter();
        let mut consumer = Consumer::builder().
            topic(topic).
            //span(&receive_span).
            bootstrap("localhost").build().await?;
        for i in 1..=count {
            let batch = consumer.recv().await.expect("Consumer closed");
            for msg in batch.messages {
                let msg = String::from_utf8(msg.value).unwrap();
                debug!(">>>Receved #{} {:?}", i, msg);
                event!(Level::INFO, %batch.high_watermark, %batch.partition, "Received {}", msg);
                assert!(sent.remove(&msg));
            }
        }
    }
    assert!(sent.is_empty());
    Ok(())
}

#[tokio::test]
async fn leader_down_producer_and_consumer_recovery() -> Result<(), Error> {
    docker_down()?;
    docker_up()?;
    let _dg = DockerGuard {};



    Ok(())
}

fn topic_can_not_be_created() {
    
}
