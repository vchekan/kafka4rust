extern crate kafka4rust;
mod utils;

use anyhow::Result;
use futures::stream::{self, StreamExt};
// use tokio_stream::{self as stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use kafka4rust::{admin, ProducerBuilder};
use kafka4rust::{ConsumerBuilder, FixedPartitioner, KafkaError};
use log::{debug, info, LevelFilter};
use rand;
use rand::seq::IteratorRandom;
use simple_logger;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::matches;
use std::thread::sleep;
use std::time::Duration;
use crate::utils::*;

mod docker;

fn random_string(len: u16) -> String {
    let mut rnd = rand::thread_rng();
    let buf = ('A'..'Z').choose_multiple(&mut rnd, len as usize);
    String::from_iter(buf.iter())
}

#[tokio::test]
async fn no_broker_resolve() {
    // Invalid host name
    let res = ProducerBuilder::new("nosuchhost.94726.nhfrt").start();
    assert!(matches!(res.err().unwrap().downcast::<KafkaError>().unwrap(), KafkaError::NoBrokerAvailable(_)));
}

/// Valid dns address exist, but broker is not listening
#[tokio::test]
async fn no_broker_connect() {
    let res = ProducerBuilder::new("localhost").start();
    assert!(matches!(res, Ok(_)));
}

/// There is one valid address among invalid ones
#[tokio::test]
async fn one_valid_address_connect() {
    let res = ProducerBuilder::new("nosuchhost.94726.nhfrt, nosuchhost.23456.nhfrt:9092, localhost").start();
    assert!(matches!(res, Ok(_)));
}

// if topic does not exists, it is created when producer connects
#[tokio::test]
async fn producer_creates_topic() -> Result<()> {
    init_log();
    info!("Starting test");
    let _d = docker::up();
    let topic = format!("test_topic_{}", random_string(5));
    let (mut p, rx) = ProducerBuilder::new("localhost").start()?;
    for i in &[1, 2, 3, 4, 5] {
        let msg = format!("msg-{}", i);
        p.send(msg, &topic).await?;
    }
    p.close().await?;

    let mut consumer = ConsumerBuilder::new(topic)
        .bootstrap("localhost")
        .build()
        .await?;
    let res: Vec<_> = ReceiverStream::new(consumer)
        .flat_map(|batch| tokio_stream::iter(batch.messages))
        .map(|msg| String::from_utf8(msg.value).unwrap())
        .take(5)
        .collect()
        .await;
    //let messages = consumer.recv().await.unwrap();
    println!("res: {:?}", res);
    assert_eq!(
        (1..=5)
            .map(|n| format!("msg-{}", n))
            .collect::<HashSet<_>>(),
        HashSet::from_iter(res.into_iter())
    );

    Ok(())
}

/*
Mutithreading save
 */

// if leader goes down, messages keep being accepted
// and are committed (can be read) within (5sec?)
// Also, order of messages is preserved
#[tokio::test]
async fn leader_down_producer_and_consumer_recovery() -> Result<()> {
    init_log();
    init_tracer()?;
    info!("Starting leader_down_producer_and_consumer_recovery()");
    let _d = docker::up();
    let topic = format!("test_topic_{}", random_string(5));
    let (mut p, rx) =
        ProducerBuilder::new("localhost").hasher(Box::new(FixedPartitioner { 0: 0_u32 })).start()?;
    for i in 1..10 {
        if i == 5 {
            let meta = admin::get_topic_metadata("localhost", &topic).await?;
            let leader = meta.topics[0].partition_metadata[0].leader;
            let leader = &meta.brokers.iter().find(|b| b.node_id == leader).unwrap();
            debug!("Sending {}", i);
            docker::hard_kill_kafka(leader.port);
        }
        let msg = format!("msg-{}", i);
        p.send(msg, &topic).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // let mut consumer = Consumer::builder().bootstrap("localhost").topic(topic).build().await.unwrap();
    // let res: Vec<_> = consumer.flat_map(|batch| stream::iter(batch.messages))
    //     .map(|msg| String::from_utf8(msg.value).unwrap())
    //     .take(50).collect().await;

    p.close().await?;

    Ok(())
}

// Timeout if no service

/*
ListenerOnNonExistentTopicWaitsForTopicCreation
 */

/*
ProducerAndListenerRecoveryTest
 */

/*
ProducerRecoveryTest
 */

/*
CanConnectToClusterAndFetchOffsetsWithBrokerDown
 */

/*
ListenerRecoveryTest
 */

/*
CleanShutdownTest
 */

/*
ConsumerFollowsRebalancingPartitions
 */

/*
KeyedMessagesPreserveOrder
 */

/*
ProducerSendBufferGrowsAutomatically
 */

/*
ExplicitOffset
 */

/*
StopAtExplicitOffset
 */

/*
StartAndStopAtExplicitOffset
 */

/*
StopAtExplicitOffsetOnEmptyTopic
 */

/*
ReadFromHead
 */

/*
       // if attempt to fetch from offset out of range, excption is thrown
       //[Test]
       //public void OutOfRangeOffsetThrows()
       //{

       //}

       //// implicit offset is defaulted to fetching from the end
       //[Test]
       //public void DefaultPositionToTheTail()
       //{

       //}
*/

/*
TopicPartitionOffsetsSerializeAndDeSerialize
 */

/*
SaveOffsetsAndResumeConsuming
 */

/*
ReadOffsets
 */

/*
TwoConsumerSubscribersOneBroker
 */

/*
MultipleProducersOneCluster
 */

/*
SchedulerThreadIsIsolatedFromUserCode
 */

/*
slow consumer
            // 1. Create a topic with 100K messages.
            // 2. Create slow consumer and start consuming at rate 1msg/sec
            // 3. ???
 */

/*
InvalidOffsetShouldLogErrorAndStopFetching
 */

/*
InvalidDnsShouldThrowException
 */

/*
OneInvalidDnsShouldNotThrow
 */

/*
SimulateLongBufferedMessageHandling
 */

/*
ProducerConnectWhenOneBrokerIsDownAndThanUp
 */

/*
ProducerTestWhenPartitionReassignmentOccurs
 */

/*
IfFirstBrokerIsDownThenNextOneWillConnect
 */

/*
memory consumption
 */

/*
       // if one broker hangs on connect, client will be ready as soon as connected via another broker

       // Short disconnect (within timeout) wont lose any messages and will deliver all of them.
       // Temp error will be triggered

       // Parallel producers send messages to proper topics

       // Test non-keyed messages. What to test?

       // Big message batching does not cause too big payload exception

       // when kafka delete is implemented, test deleting topic cause delete metadata in driver
       // and proper message error

       // Analize correlation example
       // C:\funprojects\rx\Rx.NET\Samples\EventCorrelationSample\EventCorrelationSample\Program.cs

       // Adaptive timeout and buffer size on fetch?

       // If connection lost, recover. But if too frequent, do not loop infinitely
       // establishing connection but fail permanently.

       // When one consumer fails and recovers (leader changed), another, inactive one will
       // update its connection mapping just by listening to changes in routing table and not
       // though error and recovery, when it becomes active.

       // The same key sends message to be in the same partition

       // If 2 consumers subscribed, and than unsubscribed, fetcher must stop pooling.

       // Kafka bug? Fetch to broker that has topic but not the partition, returns no error for partition, but [-1,-1,0] offsets

       // Do I need SynchronizationContext in addition to EventLoopScheduler when using async?

       // When server close tcp socket, Fetch will wait until timeout. It would be better to
       // react to connection closure immediatelly.

       // Sending messages to Producer after shutdown causes error

       // Clean shutdown doe not produce shutdown error callbacks

       // OnSuccess is fired if topic was autocreated, there was no errors, there were 1 or more errors with leaders change

       // For same key, order of the messages is preserved
*/

/*
UncompressJavaGeneratedMessage
 */

/*
JavaCanReadCompressedMessages
 */

fn init_log() {
    simple_logger::SimpleLogger::default()
        .with_level(LevelFilter::Debug)
        .init().unwrap();
}
