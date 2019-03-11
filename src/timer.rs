use std::thread;
use futures::channel::mpsc;
use std::time::Duration;

pub(crate) fn new(interval: Duration) -> mpsc::UnboundedReceiver<()> {
    let (tx, rx) = mpsc::unbounded();
    thread::Builder::new().
        name("kafka4rust timer".to_string()).
        spawn(move || {
            loop {
                thread::sleep(interval);
                tx.unbounded_send(()).unwrap();
            }
        }).unwrap();
    rx
}