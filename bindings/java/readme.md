## JNI
* Arrays are copied
* Strings are copied

## Async design
Use java's `Future` as the most common denominator. I can not call java callback from Rust async completion because JNIEnv
must be called from a thread attached to JVM. This leaves 2 options: attach every thread in Tokio pool to JVM or dedicate
a single response thread with a channel.

### How nio's epool works?
A: it is plain blocking thread with native wakeup
 
## Speed up
* GetPrimitiveArrayCritical
* NewDirectByteBuffer (SINCE: JDK/JRE 1.4)
* extended by java.nio.ByteBuffer

