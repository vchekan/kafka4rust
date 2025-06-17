use crate::connection::BrokerConnection;
use crate::error::BrokerFailureSource;
use crate::meta_cache::MetaCache;
use crate::types::BrokerId;
use crate::SslOptions;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use tracing::{debug, error, instrument, warn};
use futures::stream::FuturesUnordered;
use std::future::Future;
use futures::StreamExt;

/// When new connection object is created, where should be network connection performed?
/// 1. Add object as "connecting" to the pool and skip it until it is ready.
/// 2. Add "unconnected" state to connection object, lend it and connect upon first request,
///     it will connect and execute request.
pub(crate) struct ConnectionPool {
    // TODO: implement connection guard so it can not be lost and status is stack as "lended" forever.
    connections: HashMap<BrokerId, Entry>,
    meta_cache: MetaCache,
    connecting: FuturesUnordered<ConnectFuture>,
    ssl_options: SslOptions,
}

#[derive(Debug)]
pub(crate) enum EventType {
    NewConnection,
    FailedToConnect,
}

type ConnectFuture = Pin<Box<dyn Future<Output=Result<BrokerConnection, (BrokerId,BrokerFailureSource)>> + Send>>;

pub(crate) enum Entry {
    Available(Box<BrokerConnection>),
    Lent,
    Connecting,
}

impl Debug for ConnectionPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConnectionPool[{}]", self.connections.len())
    }
}

impl ConnectionPool {
    pub(crate) fn new(meta_cache: MetaCache, ssl_options: SslOptions) -> ConnectionPool {
        ConnectionPool {
            connections: HashMap::new(),
            meta_cache,
            connecting: FuturesUnordered::new(),
            ssl_options
        }
    }

    pub(crate) fn get(&mut self, broker_id: &BrokerId) -> Entry {
        match self.connections.get_mut(broker_id) {
            Some(entry) => {
                match entry {
                    Entry::Available(conn) => std::mem::replace(entry, Entry::Lent),
                    Entry::Lent => Entry::Lent,
                    Entry::Connecting => Entry::Connecting
                }
            }
            None => {
                self.new_connection(*broker_id);
                Entry::Connecting
            }
        }
    }

    pub(crate) fn has(&self, id: &BrokerId) -> bool {
        self.connections.contains_key(id)
    }

    #[instrument(level = "debug", skip(self))]
    pub(crate) fn return_conn(&mut self, conn: Box<BrokerConnection>) {
        let broker_id = conn.broker_id;
        match self.connections.insert(conn.broker_id, Entry::Available(conn)) {
            Some(Entry::Connecting) => error!("Returning connection for Id which is connecting: {}", broker_id),
            Some(Entry::Available(_)) => error!("Returning connection for Id which already exists: {}", broker_id),
            _ => {}
        }
    }

    #[instrument(level = "debug")]
    fn new_connection(&self, broker_id: BrokerId) {
        match self.meta_cache.get_addr_by_broker(&broker_id) {
            Some(addr) => {
                debug!("Connecting to broker {broker_id}/{addr}");
                let ssl_options = self.ssl_options.clone();
                self.connecting.push(Box::pin(async move {
                    match BrokerConnection::connect(addr, broker_id, &ssl_options).await {
                        Ok(conn) => {
                            debug!(name: "Connected", %broker_id, addr = %conn.addr);
                            Ok(conn)
                        }
                        Err(e) => {
                            Err((broker_id, e))
                        }
                    }
                }));
            }
            None => {
                todo!()
            }
        }
        
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.connecting.is_empty()
    }

    #[instrument(level = "debug", skip(self))]
    pub(crate) async fn drive(&mut self) -> EventType {
        match self.connecting.next().await {
            Some( Ok(conn)) => {
                debug!("connected {}", conn.addr);
                self.connections.insert(conn.broker_id, Entry::Available(Box::new(conn)));
                EventType::NewConnection
            }
            Some(Err((broker_id, err))) => {
                debug!(name: "Failed to connect", conn = broker_id, %err);
                let _ = self.connections.remove(&broker_id);
                EventType::FailedToConnect
            }
            None => { 
                warn!("Shouldn't be called on empty connections"); 
                EventType::FailedToConnect
            }
        }
    }
}

