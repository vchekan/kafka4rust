//! Problem: both rust and .net keep stateful objects for client representation.
//! Rust should know how to call .net task completion. Keep track of .net opaque vtable struct.
//! Pushback and blocking calls interference: dispatch callbacks on dedicated thread pool.
//!
//! .net originates and destroys objects.
//!
//! Cancellation from .net
//!
//! panic handling
use kafka4rust::{Producer, BinMessage};
use tokio;
use lazy_static::lazy_static;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::ptr::null;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock};
use log::debug;

lazy_static! {
    static ref TOKIO_RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
    static ref MANAGED_PRODUCERS: RwLock<HashMap<usize,Mutex<Producer>>> = RwLock::new(HashMap::new());
}

static PRODUCER_ID: AtomicUsize = AtomicUsize::new(1);

type DotnetCallback = extern fn(result: Result);
type CreateProducerCallback = extern fn(result: CreateProducerResult);

#[repr(C)]
pub struct OpaqueDotnetPointer(*mut Ptr);
pub struct Ptr;
unsafe impl Send for OpaqueDotnetPointer {}

#[repr(C)]
pub struct Result {
    ptr: *const c_void,
    continuation: OpaqueDotnetPointer,
    error: bool,
}

#[repr(C)]
pub struct CreateProducerResult {
    msg: *const u8,
    data: usize,
    continuation: OpaqueDotnetPointer,
    error: bool,
}

#[repr(C)]
pub struct SyncResult {
    ptr: *const c_void,
    msg: *const u8,
    error: bool,
}

#[no_mangle]
pub extern fn create_producer(continuation: OpaqueDotnetPointer, bootstrap: *const c_char, callback: CreateProducerCallback) {
    // TODO: catch all panics
    // TODO: implement `DotnetString` with `!Send`.
    // Dotnet string is valid only during synchronous part of the call and can be reclaimed after call completion.
    // So it is unsafe to pass the pointer to thread and operate on it after call is completed.
    // String must be cloned if pass to thread is required.
    let bootstrap = match unsafe { CStr::from_ptr(bootstrap) }.to_str().map(String::from) {
        Ok(bootstrap) => bootstrap,
        Err(e) => {
            println!("rust:create_producer: failed utf8: {:#}", e);
            let msg = format!("Failed to convert bootstrap into utf8 string: {:#}\0", e);
            let res = CreateProducerResult {
                msg: msg.as_ptr(),
                data: 0,
                continuation,
                error: true,
            };
            callback(res);
            println!("rust:create_producer: failed utf8: returning");
            return;
        }
    };

    println!("rust:create_producer: {}", bootstrap);

    TOKIO_RUNTIME.spawn( async move {
        println!("rust: create_producer: before runtime");
        println!("rust: got param: {}", bootstrap);
        match Producer::new(&bootstrap) {
            Ok((producer, rx)) => {
                println!("Connected {:?}", producer);
                let id = PRODUCER_ID.fetch_add(1, Ordering::SeqCst);
                MANAGED_PRODUCERS.write().await.insert(id, Mutex::new(producer));
                callback(CreateProducerResult {
                    msg: 0 as *const u8,
                    data: id,
                    continuation,
                    error: false,
                })
            },
            Err(e) => {
                println!("rust error: {:#}", e);
                let msg = CString::new(format!("{:#}\0", e)).expect("Failed to convert String to CString\0");
                let msg = msg.as_ptr() as *const u8;
                callback(CreateProducerResult { error: false, msg, data: 0, continuation })
            }
        }
    });
}

// TODO: can be called from multiple threads, need synchronization?
#[no_mangle]
pub extern fn send(producer_id: usize, topic: *const c_char,
                   key: *const u8, key_len: u32, val: *const u8, val_len: u32,
                   continuation: OpaqueDotnetPointer, send_callback: DotnetCallback) {
    let topic  = match unsafe {CStr::from_ptr(topic)}.to_str().map(String::from) {
        Ok(topic) => topic,
        Err(e) => {
            let msg = format!("Failed to convert topic into utf8 string: {:#}\0", e);
            send_callback(Result {
                error: true,
                ptr: msg.as_ptr() as *const c_void,
                continuation
            });
            return;
        }
    };

    let mut key_owned =  Vec::<u8>::with_capacity(key_len as usize);
    unsafe { key_owned.as_mut_ptr().copy_from(key, key_len as usize); }
    let mut val_owned = Vec::<u8>::with_capacity(val_len as usize);
    unsafe { val_owned.as_mut_ptr().copy_from(val, val_len as usize); }

    TOKIO_RUNTIME.spawn(async move {
        debug!("rs:send");
        log::logger().flush();
        println!("print-debug");

        let msg = BinMessage {
            key: key_owned,
            value: val_owned,
        };

        match MANAGED_PRODUCERS.read().await.get(&producer_id) {
            Some(producer) => {
                let mut producer = producer.lock().await;
                // TODO: handle result
                match producer.send(msg, &topic).await {
                    Ok(_) => {
                        send_callback(Result {
                            error: false,
                            ptr: null(),
                            continuation,
                        });
                    }
                    Err(e) => {
                        send_callback(Result {
                            error: true,
                            ptr: format!("Send failed: {:#}\0", e).as_ptr() as *const c_void,
                            continuation,
                        });
                    }
                }
            },
            None => {
                send_callback(Result {
                    error: true,
                    ptr: format!("Invalid producer id: {}\0", producer_id).as_ptr() as *const c_void,
                    continuation,
                });
                return;
            }
        };
    });
}

#[no_mangle]
pub extern fn close(producer_id: usize, continuation: OpaqueDotnetPointer, callback: DotnetCallback) {
    debug!("r:close called id: {}", producer_id);
    TOKIO_RUNTIME.spawn(async move {
        match MANAGED_PRODUCERS.write().await.remove(&producer_id) {
            Some(producer) => {
                match producer.into_inner().close().await {
                    Ok(_) => {
                        debug!("r:closed producer OK");
                        callback(Result{error: false, ptr: null(), continuation})
                    },
                    Err(e) => {
                        debug!("rs:close:error: {:#}", e);
                        callback(Result{error: true, ptr: format!("{:#}\0", e).as_ptr() as *const c_void, continuation})
                    }
                }
            }
            None => {
                callback(Result {error: true, ptr: format!("Invalid producer id: {}\0", producer_id).as_ptr() as *const c_void, continuation});
            }
        };

    });
}

#[no_mangle]
pub extern fn init_log() {
    use log::debug;
    simple_logger::init_with_level(log::Level::Debug).expect("Failed to init logging");
    debug!("Test debug level");
}