//! Type aliases
pub(crate) type BrokerId = i32;
pub(crate) type Partition = u32;
pub(crate) type Offset = u64;

use std::net::SocketAddr;
use std::fmt;

/// Represents a broker address with both IP and hostname for TLS connections
#[derive(Clone, Debug)]
pub(crate) struct BrokerAddress {
    pub(crate) addr: SocketAddr,
    pub(crate) hostname: String,
}

impl fmt::Display for BrokerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.hostname)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TopicMeta {
    pub topic: String,
    pub partitions: Vec<Option<PartitionMeta>>
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionMeta {
    leader: BrokerId,
}

impl Into<TopicMeta> for &crate::protocol::TopicMetadata {
    fn into(self) -> TopicMeta {
        TopicMeta {
            topic: self.topic.clone(),
            // TODO: assert partition order match to partition Id
            partitions: self.partition_metadata.iter().map(|p| {
                Some(PartitionMeta {
                    leader: p.leader,
                })
            }).collect()
        }
    }
}

impl Into<PartitionMeta> for &crate::protocol::PartitionMetadata {
    fn into(self) -> PartitionMeta {
        PartitionMeta {
            leader: self.leader
        }
    }
}

/// Serialized message with topic and partition preserved because we need them in case topic
/// resolved or topology change.
#[derive(Debug)]
pub(crate) struct QueuedMessage {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: u64,
}

impl QueuedMessage {
    pub fn size(&self) -> usize {
        self.value.len() + 4 +
            match &self.key {
                Some(key) => key.len() + 4,
                None => 4
            }
            + 8 // timestpamp

    }
}