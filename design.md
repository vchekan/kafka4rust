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
Send buffer should be accessible for write to be able to accept new messages without delay and at the same time, it 
should be able 

## Message based design
Locking is hard to do without slowing down things. Let's see either message based design will solve it.
