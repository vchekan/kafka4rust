# Design considerations

## Topic Resolver
Topic resolver fetches partition leader mapping for given topic.
It plays an important role in recovery after partitions have been rebalanced by the cluster.
First we need to decide what is rebalancing error. If we failed to connect to partition's leader or if error returned 
suggests that broker is not a leader for given partition, then we want to fetch topic metadata again and update 
partition to broker mapping.

Constraints: It is desirable to suspend activity only on affected partitions and continue operations on non-affected.

Also, when multiple partitions will fail, only single metadata request should be sent. 

How to design Rust components for this?
Flushing is happening inside a flushing loop. The flushing loop owns buffer and communicates to Producer via channel.
Buffer and Cluster are protected by Mutex. Buffer, because 2 components access it mutably: Producer to write new data
and flush loop to discard written data. 
When Buffer's flush function decides that error requires metadta refresh, it marks partition in buffer's queue as 
stalled and requests from Cluster new partition info.

But this design will violate "one metadata request" requirement because it will issue multiple metadata requests, one 
for every filed partition, even if they can be resolved by single request.
To mitigate this, a set of topics currently being resolved is added. If given topic is added to the set, metadata 
request will be issued, otherwise ignored, because request is already in-progress.

### Locking characteristics
Presence of mutexes raise question about performance.
Both Buffer and Cluster locks are held for the duration of flushing, which is not good for sending data buffer lock.
Cluster is mutable because `Cluster::broker_get_or_connect` mutates cluster in case if new connection is required.
During `flush` execution, which can be quite long (seconds) no adding to buffer is possible nor connection to a new 
broker. In order to minimize locking time, we will switch to RwLock,  


## Topic Resolver, try 2
Parts: 
* Consumer/Producer
* Cluster
* fetch loop


## Send Buffer mutability design
Send buffer have the following access patterns:
* Write, short, from application thread
* Read, long from flushing loop. Long because reference to enqueued messages are kept during sending. 

## Message based design
Locking is hard to do without slowing down things. Let's see either message based design will solve it.

## Locks notes
How to work on broker map while be able to update it?
* Rw lock.
* Immutable maps 
* Lock-free (crossbeam epoch)

Problem: it is desirable to return `&Broker` instead of cloning `Broker`, which means complication to locking scope. And
`Broker` is used to send request, which means really long locking time!

### RWLock
To solve long-lived `&Broker`, looks like Arc is needed to return broker from the map.

### Immutable map
When broker found, return `&Broker` which ties lifetime to `self`.
When broker not found, create a new one and add to a map, which will create new immutable map object. Swap pointers. Wait,
this requires mutable references which makes it no go. 
`AtimicPtr` does allow operation on immutable `self` but puches us into pointer operations and dereferencing pointers is 
unsafe. In addition to this, we can not leak old map nor drop it, because there might be other threads/futures still 
using it. This could be addressed by `crossbeam::epoch`.

### Lock-free (crossbeam::epoch)
Adaptation of Immutable Map approach but put old map into epoch collector. Now, how do we deal with returning `&Broker`?
The problem is, thread T1 loads map pointer and return `&Broker`. At the same time thread T2 misses a broker, creates a 
new one, replaces map pointer with a new one and put old broker into delayed drop queue. Now old queue can be dropped 
and cause `Broker` drop while `&Broker` is still shared?

Another question: is epoch future-compatible?

# Actor architecture
Should BrokerConnection operate with byte arrays or kafka messages? Byte array is simple but error code analysis can't 
be performed until message is sent back to the Buffer.

Sending kafka request via channel is not easy. What would be type of message to be able to carry multiple types of 
requests? Possibly `channel::<Box<dyn ToKafka>>()` with some additional trait to get response type from request type. But
even then, how to call appropriate deserialize generic function, knowing a type?

But even with Actor architecture, it is easy to have Lock's architecture problem, for example:
```rust
impl Cluster {
    fn get_or_fetch_metadata(&mut self, topic: &str) {
        // if in self.cache, get it
        //otherwise, connect and fetch
        self.connect_to_any_broker_and_fetch_metadata().await;
    }
}
```

Fetching to a broker and getting metadata might take long time, especialy if some failure recovery is done by the 
cluster. As `&mut self` is in scope, it means, that no other message can be processed, including flushing buffers which 
do have all needed metadata.

The solution is a Message Bus architecture. When Buffer misses metadata, it sends request to Cluster but does not wait
for response. Cluster relays metadata request to a connection, and does not await for response either. Instead metadata 
is published on a bus and any interested party can update their topology. 

## Components and interaction
### Initialization
Client adds a message to the Buffer.
Buffer detects misses in leader map for this topic and send "meta_needed(topic)" to the cluster.
Cluster keeps a list of topics resolved and "in progress", and if a miss, sends message to Resolver.
Resolver adds (if missing) topic to the list of topics to be resolved.
Resolver periodically issue "fetch_meta" command to the list of brokers from bootstrap + known brokers.
Resolver keeps and updates list of known brokers by analyzing responses.
Resolver sends to the Cluster Ok(meta) or Err(TopicNotFound(topic)) and removes topic from the list. If request failed
  or LeaderNotFound then do nothing and retry next time.
Cluster sends LeaderMap to Buffer.
Buffer keeps leader map to be able to partrition messages when "flush()" .

### Failure recovery (transport error)
Tcp connection is reset or send times out. ConnectionBroker sends "LeaderDown(leaderId)" to Cluster.  
Cluster disposes the connection, removes it from maps and sends LeaderMap to Buffer.
Cluster sends message to Resolver with all topics which were handled by this broker at least partially.

### Failure recovery (kafka error code)
Connection performs exchange and deserialization. If there is an error NotLeaderForPartition then send message to 
  Cluster (PartitionDown(topic, partition))
Cluster marks topic/partition as "down", sends LeaderMap to Buffer and recovery requst to Resolver. 

### Buffer flushing
Buffer gets timer event, or message itself when size threshold is exceeded.
Buffer groups messages by leader, according to leader map.
Buffer sends group to Cluster Produce(grouped_messages, respond_to).
  ??? What is the type of respond_to?
Cluster routes messages to appropriate Connection. If leader is missing, Nack is sent to Buffer (possible if LeaderMap 
  update has been sent but not processed by Buffer yet).
Connection performs exchange. 
    If tcp error happens, Nack is sent to respond_to.
    If NotLeaderForPartition send Cluster PartitionDown()
    If any partition or the whole message error then send Nack(topic, Partition)
    If success then send Ack(topic,partition) to respond_to
    
### Components
#### Resolver
Resolves multiple topics at the time. Reasoning: when failure happen, multiple topics will break and making a query for 
every one of them does not make sense.  

#### Cluster 
Cluster keeps list of known brokers and topics leader map.

### Metadata broadcasting
Topic metadata is used by different components, Cluster needs it to group messages by leader, Resolver needs it to connect to all
known brokers, Producer needs partitions count to partition message by the key.

One option is to build a hierarchy of metadata propagation, Resolver sends meta to Cluster, Cluster to Producer, Producer to Buffer.
Another option is to build broadcast system, where Resolver broadcast meta updates to every subscriber.

### Producer: should connect in constructor or not?
Producer needs to have patrition count in order to be able to perform key partitioning. So it makes no sense to make
Producer available before topic is resolved. On the other hand, if we want to publish to any topic, and make topic part
of `send()` api, then we would have to resolve topic metadata during `send()`.

On the other hand there are legitimate patterns when publishing topic is calculated dynamically for every message, for 
example request-response pattern.

So it is necessary to figure out how to wait for topic info to arrive in Producer. The challenge is, Producer must stop 
processing until topic is resolved but it needs to process.

Waiting for single topic to resolve while processing everything else is complicated. So simpler workflow could be used.
When new topic is observed, resolve request is sent, while messages are put into "unresolved" queue. Once topic is resolved,
Producer scans "unresolved" queue and puts messages into per-partition queue structure. If "unresolved" queue is overflow,
then it awaits for partition to resolve. As implemented in Producer, it will not freeze Buffer background flushing but will
freeze sending data by caller, which is acceptable and expected. 
TODO: make sure that Buffer->Producer communication does not freeze Buffer background flushing.

## Short vs long living async operations
Not all async are equal. If long-lived async, such as discovery of cluster repartitioning, is called from short-lived,
such as actor event handling, then things will go wrong. Timeouts, deadlocks, performance degradation.

So you should not be able to call long-living async from short-living one. Long-living should be called as fire-and-forget
with respond-to caller's `handle` method.

But fire-and-forget is not very convenient to use. You have to define request message, response message, respond_to field
in request message, and we have not even touched cancellation yet.