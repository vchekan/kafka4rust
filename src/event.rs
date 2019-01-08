use futures::Future;
use futures::future::FusedFuture;
use core::task::{Poll, LocalWaker};
use core::task::Waker;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex as StdMutex;
use std::usize;
use slab::Slab;
use std::mem;

pub struct Event {
    state: AtomicBool,
    waiters: StdMutex<Slab<Option<Waker>>>
}

impl Event {
    pub fn new(state: bool) -> Self {
        Event {
            state: AtomicBool::new(state),
            waiters: StdMutex::new(Slab::new())
        }
    }

    pub fn set(&self) {
        debug!("Setting event");
        if self.state.compare_and_swap(false, true, Ordering::SeqCst) == false {
            // If set flag, check either there are any waiter and wake the first one
            // TODO: make flag AtimicPointer and save "have waiters" bit to skip
            // waiters queue locking if there are no waiters
            let mut waiters = self.waiters.lock().unwrap();
            match waiters.iter_mut().next() {
                Some((_i,waiter)) => {
                    match mem::replace(waiter, None) {
                        Some(waiter) => {
                            debug!("Waking waiter because flag was set");
                            waiter.wake();
                            debug!("Water woken. Queue len: {}", waiters.len());
                        },
                        None => {
                            // TODO: should I panic because next waiter will never be called?
                            debug!("Waiter was already woken");
                        }
                    }
                },
                None => {
                    debug!("No waiters");
                }
            }
        }
    }

    /*pub fn reset() {

    }*/

    pub fn wait(&self) -> EventFuture {
        EventFuture{ event: Some(self), wait_key: WAIT_KEY_NONE}
    }

    fn remove_waker(&self, key: usize) {
        debug!("remove_waker({})", key);
        if key != WAIT_KEY_NONE {
            let mut waiters = self.waiters.lock().unwrap();
            let res =  waiters.remove(key);
            if waiters.is_empty() {
                
            }
        }
    }
}

pub struct EventFuture<'a> {
    event: Option<&'a Event>,
    wait_key: usize
}

impl FusedFuture for EventFuture<'_> {
    fn is_terminated(&self) -> bool { self.event.is_none() }
}

const WAIT_KEY_NONE: usize = usize::MAX;

impl<'a> Future for EventFuture<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, lw: &LocalWaker) -> Poll<Self::Output> {
        // TODO: update waker if already waiting
        debug!("poll() called");
        let event = self.event.expect("polled EventFuture after completion");
        // TODO: think about ordering, can it be relaxed?
        let flag = event.state.load(Ordering::SeqCst);
        if flag {
            debug!("Flag is up, removing waker[{}] and finishing", self.wait_key);
            self.event = None;
            event.remove_waker(self.wait_key);
            Poll::Ready(())
        } else {
            // Flag is down. Add self to waiting queue if not already
            if self.wait_key == WAIT_KEY_NONE {
                // TODO: can waiter be awoken after this insert before self.key is assigned?
                // TODO: can flag be set after previous check and no `wake` be called anymore?
                let mut waiters = event.waiters.lock().unwrap();
                debug!("Adding waiter...");
                self.wait_key = waiters.insert(Some(lw.clone().into_waker()));
                debug!("Wait queue length {}", waiters.len())
            }
            Poll::Pending
        }
    }
}

unsafe impl Send for Event {}
unsafe impl Sync for Event {}

#[cfg(test)]
mod test {
    use super::*;
    use simplelog::*;
    use futures::{executor, ready, poll, join};
    use std::sync::Arc;
    use std::{thread, time};

    #[test]
    fn test() {
        CombinedLogger::init(vec![TermLogger::new(LevelFilter::Debug, Config::default()).unwrap()]).unwrap();
        debug!("Starting");

        executor::block_on(async {
            debug!("Executor started");
            let e = Arc::new(Event::new(false));
            let ee = Arc::clone(&e);
            thread::spawn(move || {
                debug!("Starting sleep");
                thread::sleep(time::Duration::from_millis(2000));
                debug!("Finishing sleep. Setting flag");
                ee.set();
                debug!("Set flag");
            });

            debug!("Awaiting flag");
            await!(async {
                let e1 = e.wait();
                let e2 = e.wait();
                join!(e1, e2);
            });
            debug!("Awaied flag");
            assert_eq!(0, e.waiters.lock().unwrap().len(), "Waiters queue is not empty");
        });
    }
}