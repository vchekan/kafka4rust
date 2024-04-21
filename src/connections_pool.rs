use crate::connection::BrokerConnection;
use crate::meta_cache::MetaCache;
use crate::types::BrokerId;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use tracing::error;

/// When new connection object is created, where should be network connection performed?
/// 1. Add object as "connecting" to the pool and skip it until it is ready.
/// 2. Add "unconnected" state to connection object, lend it and connect upon first request,
///     it will connect and execute request.
pub(crate) struct ConnectionPool {
    connections: HashMap<BrokerId, Entry>,
    // connecting: &'a FuturesUnordered<BrokerResult<BrokerConnection>>,
}

pub(crate) enum Entry {
    Available(Box<BrokerConnection>),
    Lent,
    Resolving,
    Connecting,
}

impl Debug for ConnectionPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConnectionPool[{}]", self.connections.len())
    }
}


impl ConnectionPool {
    pub(crate) fn new() -> ConnectionPool {
        ConnectionPool {
            connections: HashMap::new()
        }
    }

    fn add(&mut self, conn: BrokerConnection, id: BrokerId) {
        if let Some(_) = self.connections.insert(id, Entry::Available(Box::new(conn))) {
            error!("add: Message already exists");
        }
    }

    pub(crate) fn take(&mut self, id: &BrokerId) -> Option<Entry> {
        match self.connections.get_mut(id) {
            Some(entry) => {
                match entry {
                    Entry::Available(_) => Some(std::mem::replace(entry, Entry::Lent)),
                    _ => None,
                }
            }
            None => None
        }
    }

    pub(crate) fn has(&self, id: &BrokerId) -> bool {
        self.connections.contains_key(id)
    }

    /// Set missing BrokerID as resolving
    pub(crate) fn set_resolving(&mut self, id: BrokerId) {
        if self.connections.contains_key(&id) {
            error!("Setting 'Resolving' to broker id which is already set: {}", id);
            return;
        }
        self.connections.insert(id, Entry::Resolving);
    }

    fn return_conn(&mut self, conn: Box<BrokerConnection>, id: BrokerId) {
        if let Some(_) = self.connections.insert(id, Entry::Available(conn)) {
            error!("Returned connection for Id which already exists: {}", id);
        }
    }

    pub fn update_meta(&mut self, meta: &MetaCache) {
        todo!()
    }
}

