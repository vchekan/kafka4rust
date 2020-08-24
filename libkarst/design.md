# Design
## Ownership
Ownership is controlled by host language. Rust "leaks" resources to the host.
For test purposes Rust keeps statistics of "in flight" resources, so it is possible to validate that they are zero at
termination.

When it comes to data transfer, two ways are possible. One is to allocate buffers in Rust and return pointer to C#.
Another way is to allocate in c# and fill it out in Rust.  

# C#
## Resources
* "fixed" - old school
* Benchmark.NET
* https://devblogs.microsoft.com/dotnet/performance-improvements-in-net-core-3-0/
* Span, ReadOnlySpan, Memory, ReadOnlyMemory.   
    Span: ref struct, memory: struct
* Stream.WriteAsync
* "If in your code by construction you know that the span will not be empty, you can choose to instead use 
  MemoryMarshal.GetReference, which performs the same operation but without the length check"
* ref local
* ref struct
* UnsafeQueueUserWorkItem
* SafeHandle
* INotifyCompletion & ICriticalNotifyCompletion 
```
private SafeFileHandle _sfh = new SafeFileHandle((IntPtr)12345, ownsHandle: false);
public IntPtr SafeHandleOps() {
    bool success = false;
    try {
        _sfh.DangerousAddRef(ref success);
        return _sfh.DangerousGetHandle();
    } finally {
        if (success)
            _sfh.DangerousRelease();
```
* Custom async in C#
```
public static Task WaitOneAsync(this WaitHandle waitHandle) {
    if (waitHandle == null)
        throw new ArgumentNullException("waitHandle");

    var tcs = new TaskCompletionSource<bool>();
    var rwh = ThreadPool.RegisterWaitForSingleObject(waitHandle,
        delegate { tcs.TrySetResult(true); }, null, -1, true);
    var t = tcs.Task;
    t.ContinueWith( (antecedent) => rwh.Unregister(null));
    return t;
}
```

## Design
Use SafeHandle to allocate Producer/Consumer in C# and leak them in Rust.


## Pushback

## Strings

## Buffers

## Async

## Release of resources

## Panics and exceptions

## MAD: Mutually Assured Destruction
Bad:
* Rust calls destructed C# object
* C# calls dropped Rust object (memory with random content at this point)