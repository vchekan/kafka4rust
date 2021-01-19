extern crate kafka4rust;
mod utils;

use crate::utils::*;
use anyhow::Result;
use kafka4rust::{Consumer, Producer, Response};
use log::debug;
use opentelemetry::{api::Provider, global};
use rand;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::collections::HashSet;
use tokio;
use tracing::{self, dispatcher, event, span, Level};
use tracing_futures::Instrument;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

fn random_topic() -> String {
    let topic: String = rand::thread_rng()
        .sample_iter(Alphanumeric)
        .take(7)
        .collect();
    format!("test_{}", topic)
}

#[tokio::test]
async fn topic_is_autocreated_by_producer() -> Result<()> {
    simple_logger::init_with_level(log::Level::Debug)?;
    init_tracer()?;
    let tracer = global::trace_provider().get_tracer("component1");
    let otl = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(otl);
    let dispatch = dispatcher::Dispatch::new(subscriber);
    dispatcher::set_global_default(dispatch)?;

    let span = span!(tracing::Level::INFO, "topic_is_autocreated_by_producer");
    let _guard = span.enter();

    let bootstrap = "localhost";
    let topic = random_topic();
    let count = 200;
    let mut sent = HashSet::new();
    let (mut producer, mut responses) = Producer::new(bootstrap)?;

    let acks = tokio::spawn(async move {
        while let Some(ack) = responses.recv().await {
            match ack {
                Response::Ack { .. } => debug!("Ack: {:?}", ack),
                Response::Nack { .. } => assert!(false, "Got nack, send failed: {:?}", ack),
            }
        }
    });

    let span = tracing::span!(tracing::Level::ERROR, "send");
    let _guard = span.enter();
    for i in 1..=count {
        let msg = format!("m{}", i);
        sent.insert(msg.clone());
        event!(Level::INFO, %msg, "Sent");
        producer.send(msg, &topic).await?;
    }
    producer
        .close()
        .instrument(tracing::trace_span!("Closing producer"))
        .await?;
    debug!("Producer closed");

    acks.instrument(tracing::trace_span!("Awaiting for acks"))
        .await?;

    debug!("Acks received");

    {
        let span = tracing::span!(tracing::Level::ERROR, "receive");
        let _guard = span.enter();
        let mut consumer = Consumer::builder().
            topic(topic).
            //span(&receive_span).
            bootstrap("localhost").build().await?;
        let mut i = 0;
        loop {
            let batch = consumer.recv().await.expect("Consumer closed");
            i += batch.messages.len();
            for msg in batch.messages {
                let msg = String::from_utf8(msg.value).unwrap();
                debug!(">>>Receved #{}/{} {:?}", i, (count - i), msg);
                event!(Level::INFO, %batch.high_watermark, %batch.partition, "Received {}", msg);
                assert!(sent.remove(&msg));
            }
            if i >= count {
                break;
            }
        }
    }
    assert!(sent.is_empty());
    Ok(())
}

#[tokio::test]
async fn leader_down_producer_and_consumer_recovery() -> Result<()> {
    docker_down()?;
    docker_up()?;
    let _dg = DockerGuard {};

    Ok(())
}

fn topic_can_not_be_created() {}
