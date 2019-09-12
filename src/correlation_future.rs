use juliex;
use std::task::{Waker, Context, Poll};
use std::future::Future;
use std::pin::Pin;
use romio::TcpStream;
use std::collections::HashMap;

struct BrokerConnection {
    tcp: TcpStream,
    map: HashMap<u32, Waker>,
}

struct BrokerConnectionFuture {
    requests_futures: Vec<CorrelationFuture>,
}

impl Future for BrokerConnectionFuture {
    type Output = String;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        unimplemented!()
    }
}

impl BrokerConnection {
    pub fn request(&self, correlation_id: u32) -> impl Future<String> {
        let mut buf = Vec::with_capacity(1024);
        self.tcp.read(&mut buf).and_then(|buffer| {
            CorrelationFuture {
                correlation_id,
                waker: None,
            }
        });

    }
}

struct CorrelationFuture {
    correlation_id: u32,
    waker: Option<Waker>,
}

//
//
//
impl Future for CorrelationFuture {
    type Output = String;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {

        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor;

    #[test]
    fn test() {
        executor::block_on(async {
            // Declare futures in reverse correlation order
            let f1 = CorrelationFuture::new(2);
            let f2 = CorrelationFuture::new(1);

            f1.await;
            f2.await;
        });
    }
}