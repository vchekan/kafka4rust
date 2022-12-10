//! Type aliases
pub(crate) type BrokerId = i32;
pub(crate) type Partition = u32;
pub(crate) type Offset = u64;

pub(crate) struct TopicMeta {
    pub topic: String,
    pub partitions: Vec<Option<PartitionMeta>>
}

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
