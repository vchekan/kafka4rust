//! Connection should support pipelining requests. At the same time same connection should serve
//! multiple customers: recovery manager, metadata requestor, data sender. This means that same
//! connection reference should be possible to share (behind `Rc`). This implies that Connection's
//! functions should be immutable and appropriate operations locked. Despite being executed in
//! single-threaded executor, interleaving can occur in functions, such as `Tcp_Stream::write_all`
//! which would cause data corruption.
//! TODO: Q: does it guarantee that reads are in the same order as writes?
//!
//! Alternative design would be to lock Connection, write data and obtain read data future, release
//! lock and await fore read data outside of lock.
//!
//! Yet another alternative is to split TcpStream into writer and reader, but then some work is
//! needed to expose request-response as a single future. But WriteHalf does not have Clone, so it
//! is no go.
//!
//! Read loop design.
//! Read loop can set `current_response` but upon next iteration, how does it know that message has
//! been handled already? It seems like response handler driving the read is more natural.
//!
//! Write channel: how to implement sender's pushback?

use byteorder::BigEndian;
use bytes::{ByteOrder, BytesMut};
use std::io;
use std::net::{SocketAddr};

use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::sync::Mutex;
use std::sync::Arc;
use anyhow::{Context, Result, anyhow};

pub(crate) const CLIENT_ID: &str = "k4rs";

#[derive(Debug, Clone)]
pub struct BrokerConnection {
    addr: SocketAddr,
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    // TODO: move correlation here
    //correlation_id: u32,
    tcp: TcpStream,
}

impl BrokerConnection {
    /// Connect to address but do not perform any check beyond successful tcp connection.
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let tcp = TcpStream::connect(&addr).await.with_context(|| format!("BrokerConnection failed to connect to {:?}", addr))?;

        let conn = BrokerConnection {
            addr,
            inner: Arc::new(Mutex::new(Inner {
                //correlation_id: 0,
                tcp,
            })),
        };

        Ok(conn)
    }

    pub async fn request(&self, buf: &mut BytesMut) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let tcp = &mut inner.tcp;
        trace!("Sending request[{}] to {:?}", buf.len(), tcp.peer_addr());
        tcp.write_all(&buf).await.context(anyhow!("writing {} bytes to socket {:?}", buf.len(), tcp.peer_addr()))?;
        //debug!("Sent request");

        // TODO: buffer reuse
        //let mut buf = vec![];
        buf.clear();
        //debug!("Reading length...");
        // Read length into buffer
        buf.resize(4, 0_u8);
        // TODO: ensure length is sane
        tcp.read_exact(buf).await.context("Reading buff size")?;
        let len = BigEndian::read_u32(&buf);
        //debug!("Response len: {}, reading body...", len);
        buf.resize(len as usize, 0_u8);
        tcp.read_exact(buf).await?;
        //debug!("Read body [{}]", buf.len());

        // TODO: validate correlation_id
        let _correlation_id = byteorder::LittleEndian::read_u32(&buf);
        Ok(())
    }
}

/*
impl Future for RequestFuture {
    type Output = Vec<u8>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {

        Poll::Pending

        //
        // Take lock
        //
        let mut locked_queue : MutexLockFuture<WakerQueue> = self.futures_queue.lock();
        // TODO: write up safety invariants
        // TODO: make it part of state?
        let pin = unsafe {Pin::new_unchecked(&mut locked_queue)};
        let mut guard = match pin.poll(cx) {
            Poll::Pending => {
                debug!("Awaiting for lock");
                return Poll::Pending
            },
            Poll::Ready(guard) => guard,
        };


        // TODO: do not forget to update waker (or check it is the same)
        // as per `poll` requirements.
        match &self.state {
            State::Init => {
                debug!("Enqueued waker");
                guard.queue.push_back(Box::new(cx.waker().clone()));
                // Even if this is the only request, enqueue it in order for following requests
                // to know not to start reading until this one is complete.
                if guard.queue.len() == 1 {
                    debug!("First request in queue, start reading immediately");
                    let mut buf = vec![];
                    let read_future = read_response(&mut buf);
                    match read_future.poll(cx) {
                        Poll::Pending => {
                            debug!("Read not ready, Pending");
                            self.state = State::Read(read_future);
                            Poll::Pending
                        },
                        Poll::Ready((correlation_id, data)) => {
                            debug!("Read ready {}/[{}]", correlation_id, data.len());
                            self.state = State::Done;
                            // TODO: wake up the next one
                            Poll::Ready(data)
                        },
                    }
                } else {
                    // Some requests is already in the queue, just enqueue myself and wait for wake
                    self.state = State::Queued;
                    Poll::Pending
                }
            },
            /*State::Queued => {
                if waker.will_wake(guard.queue.peek_front()) {

                }
                debug!("Spurious wake")
            }*/
            /*Status::Read => {

            }*/
            State::Done => {
                panic!("Poll called after Done")
            }

            /*State::Queued => {
                match &guard.current_response {
                    None => {}
                    Some((correlation_id, data)) => {
                        if self.correlation_id == *correlation_id {
                            debug!("Correlation Id matched");
                            guard.current_response = None;
                            // TODO: design buffer lifetime
                            Poll::Ready(vec![])
                        } else {
                            // Got somebody's else correlation_id!
                            // TODO: do something less dramatic then panic
                            panic!("Mismatched correlation id. Expected: {} but got: {}", self.correlation_id, correlation_id);
                        }
                    }
                }
            }, State::Read => {

            }
            */
            _ => Poll::Pending
        }
    }
}
*/

/*
struct SequentialReader {
    inner: ReadHalf<TcpStream>,
    queue: VecDeque<Waker>,
    on_new_item : Option<Waker>,
}

impl AsyncRead for SequentialReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, io::Error>> {
        self.queue.push_back(cx.waker().clone());
        if self.queue.len() == 1 {

            match self.inner.poll_read(cx, buf) {
                Poll::Ready(res) => {
                    self.queue.pop_front();
                    // Wake next item
                    if let Some(next) = self.queue.front_mut() {
                        next.wake();
                    } else {
                        self.on_new_item = Some(cx.waker().clone());
                    }
                    Poll::Ready(res)
                },
                Poll::Pending => {
                    Poll::Pending
                }
            }
        } else {
            Poll::Pending
        }
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        read_response, write_request, ApiVersionsRequest0, ApiVersionsResponse0,
    };
    use async_std::task;
    use std::env;
    use std::io::Cursor;
    use std::net::ToSocketAddrs;
    use std::sync::Arc;

    #[test]
    fn it_works() {
        simple_logger::init_with_level(log::Level::Debug).unwrap();

        let bootstrap = env::var("kafka-bootstrap").unwrap_or("localhost:9092".to_string());
        let addr = bootstrap
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect(format!("Host '{}' not found", bootstrap).as_str());

        task::block_on(async move {
            let conn = Arc::new(BrokerConnection::connect(addr).await.unwrap());
            info!("conn: {:?}", conn);
            for _ in 0..2 {
                let conn = conn.clone();
                task::spawn(async move {
                    let request = ApiVersionsRequest0 {};
                    let mut buff = BytesMut::with_capacity(1024); //Vec::new();
                    write_request(&request, 0, None, &mut buff);
                    conn.request(&mut buff).await.unwrap();
                    let (correlation_id, versions): (_, Result<ApiVersionsResponse0>) = read_response(&mut Cursor::new(buff));
                    debug!(
                        "correlationId: {}, versions: {:?}",
                        correlation_id, versions
                    );
                });
            }
        });
    }
}
