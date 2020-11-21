use jni::{JNIEnv, JavaVM};
use jni::objects::{JClass, JString, JObject, JValue};
use jni::sys::{jstring, jlong, jobject, jint, JNI_VERSION_1_8};
use std::sync::atomic::{Ordering, AtomicUsize};
use tokio::sync::{Mutex, RwLock};
use kafka4rust::{Producer, StringMessage};
use lazy_static::lazy_static;
use std::collections::HashMap;
use jni::signature::{JavaType, Primitive};
use std::task::{Context, Poll};
use std::sync::Arc;
use std::thread;
use std::future::Future;
use log::{debug, error};
use tokio::task::JoinError;

lazy_static! {
    static ref TOKIO_RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
    static ref MANAGED_PRODUCERS: RwLock<HashMap<usize,Mutex<Producer>>> = RwLock::new(HashMap::new());
}

static PRODUCER_ID: AtomicUsize = AtomicUsize::new(1);

#[no_mangle]
pub extern fn JNI_OnLoad(vm: JavaVM, _reserved: usize) -> jint {
    if let Err(e) = simple_logger::SimpleLogger::from_env().init() {
        eprintln!("Failed to init rust logger: {:#}", e);
    }

    let v = vm.get_env().unwrap().get_version().unwrap();
    debug!(">OnLoad: vm: {:?}", v);

    // Have not really checked every API I am using, perhaps I can use lower version.
    // Use 1.8 just to be safe.
    JNI_VERSION_1_8
}

#[no_mangle]
pub extern fn Java_org_karst_Karst_createProducer(env: JNIEnv, _klass: JClass, bootstrap: JString) -> jlong {
    let bootstrap: String = env.get_string(bootstrap).expect("Failed to get bootstrap string").into();
    debug!("rust:create_producer: {}", bootstrap);

    let mut res = TOKIO_RUNTIME.spawn( async move {
        match Producer::new(&bootstrap) {
            Ok((producer, rx)) => {
                debug!("Connected {:?}", producer);
                let id = PRODUCER_ID.fetch_add(1, Ordering::SeqCst);
                MANAGED_PRODUCERS.write().await.insert(id, Mutex::new(producer));
                debug!("Got producer id: {}", id);
                Ok(id)
            },
            Err(e) => Err(e),
        }
    });

    match blocking_await(res) {
        Ok(Ok(res)) => res as i64,
        Ok(Err(e)) => {java_throw(&env, &format!("Connect error: {:#}", e)); -1},
        Err(e) => {java_throw(&env, &format!("Join error: {:#}", e)); -1},
    }
}

#[no_mangle]
pub extern fn Java_org_karst_Karst_producerSendString(env: JNIEnv, _klass: JClass, producerId: jlong, topic: JString, key: JString, value: JString) {
    let producerId = producerId as usize;
    let key: String = env.get_string(key).expect("Getting key string failed").into();
    let value: String = env.get_string(value).expect("Getting value string failed").into();
    let topic: String = env.get_string(topic).expect("Getting topic string failed").into();
    let mut res = TOKIO_RUNTIME.spawn( async move {
        let producer = MANAGED_PRODUCERS.read().await;
        let mut producer = producer.get(&producerId).expect("Invalid producer Id")
            .lock().await;
        producer.send(StringMessage {key, value}, &topic).await
    });

    match blocking_await(res) {
        Ok(Ok(())) => debug!("Sent msg"),
        // TODO: throw java exception
        Err(e) => java_throw(&env, "Join error"),
        Ok(Err(e)) => java_throw(&env, &format!("Send error: {:#}", e)),
    }
}

#[no_mangle]
pub extern fn Java_org_karst_Karst_producerClose(env: JNIEnv, _klass: JClass, producer_id: jlong) {
    let producer_id = producer_id as usize;
    let mut res = TOKIO_RUNTIME.spawn( async move {
        let mut producer = MANAGED_PRODUCERS.write().await;
        let producer = producer.remove(&producer_id);
        let mut producer = producer.expect("Invalid producer Id").into_inner();
        // TODO: handle error
        producer.close().await
    });

    match blocking_await(res) {
        Ok(Ok(())) => debug!("Closed producer {}", producer_id),
        // TODO: throw java exception
        Err(e) => java_throw(&env, &format!("Error joining: {:#}", e)),
        Ok(Err(e)) => java_throw(&env, &format!("Error closing producer: {:#}", e)),
    }
}

// Mini-executor.
// Wait for span completion blocking the current thread.
fn blocking_await<F: Future>(f: F) -> F::Output {
    let current_thread = std::thread::current();
    pin_utils::pin_mut!(f);
    let waker = waker_fn::waker_fn(move || {current_thread.unpark()});
    let cx = &mut Context::from_waker(&waker);
    loop {
        match f.as_mut().poll(cx) {
            Poll::Pending => thread::park(),
            Poll::Ready(res) => return res,
        }
    };
}

fn java_throw(env: &JNIEnv, msg: &str) {
    env.throw_new("org/karst/KarstException", msg).expect("Failed to throw exception");
}