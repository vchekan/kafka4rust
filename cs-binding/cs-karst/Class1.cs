using System;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace cs_karst
{
    public class Program {
        static void Main(string[] args) {
            Native.init_log();
            testProducer().Wait();
        }

        static async Task testProducer() {
            var producer = new Producer();
            await producer.Connect("localhost:9092");
            await producer.Send("test1", Encoding.UTF8.GetBytes("k1"), Encoding.UTF8.GetBytes("v1"));
            await producer.Close();
        }
    }

    public delegate void AckDelegate(byte[] key);
    public delegate void AsyncDelegate(Result result);
    public delegate void AsyncCreateProducerDelegate(CreateProducerResult result);

    [StructLayout(LayoutKind.Sequential)]  
    public struct Result {
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        public String errorMsg;
        public Action continuation;
        [MarshalAs(UnmanagedType.I1)]
        public bool IsError;  
    }

    [StructLayout(LayoutKind.Sequential)]  
    public struct SyncResult {
        public IntPtr rustObject;
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        public String errorMsg;
        [MarshalAs(UnmanagedType.I1)]
        public bool IsError;  
    }

    [StructLayout(LayoutKind.Sequential)]  
    public struct CreateProducerResult {
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        public String errorMsg;
        public IntPtr producerId;
        public Action continuation;
        [MarshalAs(UnmanagedType.I1)]
        public bool IsError;  
    }

    public class KarstException : Exception {
        public KarstException(String msg) : base(msg) {}
    }

    //
    // Producer Send awaiter
    //

    public class SendAwaiter : INotifyCompletion {
        IntPtr producer;
        String topic;
        byte[] key;
        byte[] val;

        public bool IsCompleted {get {return false;} }

        public SendAwaiter(IntPtr producer, String topic, byte[] key, byte[] val) {
            this.producer = producer;
            this.topic = topic;
            this.key = key;
            this.val = val;
        }
        
        public void GetResult() {}
        
        public unsafe void OnCompleted(Action continuation) {
            fixed (byte* key = this.key, val = this.val) {
                Console.WriteLine("SendAwaiter:OnCompleted");
                Native.send(producer, topic, key, this.key.Length, val, this.val.Length, continuation, (Result r) => {
                    if(r.IsError)
                        throw new KarstException(r.errorMsg);
                    Console.WriteLine("Send complete");
                    continuation();
                });
            }
        }
    }

    public class SendAwaitable {
        IntPtr producer;
        String topic;
        byte[] key;
        byte[] val;
        public SendAwaitable(IntPtr producer, String topic, byte[] key, byte[] val) {
            this.producer = producer;
            this.topic = topic;
            this.key = key;
            this.val = val;
        }
        public SendAwaiter GetAwaiter() { return new SendAwaiter(producer, topic, key, val); }
    }

    //
    // Producer Create awaiter
    //

    public class CreateAwaiter : INotifyCompletion {
        String bootstrap;
        IntPtr producer;

        public CreateAwaiter(String bootstrap) {this.bootstrap = bootstrap;}
        public bool IsCompleted {get {return false;}}
        public IntPtr GetResult() {return producer;}
        public void OnCompleted(Action continuation) {
            Native.create_producer(continuation, bootstrap, r => {
                if(r.IsError)
                    throw new KarstException(String.Format("c#: CreateProducer result error: {0}", r.errorMsg));
                this.producer = r.producerId;
                r.continuation();
            });
        }
    }

    public class CreateAwaitable {
        String bootstrap;
        public CreateAwaitable(String bootstrap) {this.bootstrap = bootstrap;}
        public CreateAwaiter GetAwaiter() { return new CreateAwaiter(bootstrap); }
    }

    //
    // Producer close awaitable
    //

    public class CloseAwaiter : INotifyCompletion {
        IntPtr producer;
        public CloseAwaiter(IntPtr producer) {this.producer = producer;}
        public bool IsCompleted {get {return false;}}
        public void GetResult() {}
        public void OnCompleted(Action continuation) {
            Native.close(producer, continuation, r => {
                if(r.IsError)
                    throw new KarstException($"Close failed. Error: {r.errorMsg}");
                r.continuation();
            });
        }
    }

    public class CloseAwaitable {
        IntPtr producer;
        public CloseAwaitable(IntPtr producer) {this.producer = producer;}
        public CloseAwaiter GetAwaiter() {return new CloseAwaiter(producer);}
    }

    //
    // Producer
    //

    public class Producer {
        private IntPtr producer;

        // API
        public async Task Connect(String bootstrap) {
            var res = await new CreateAwaitable(bootstrap);
            this.producer = res;
        }

        public async Task Send(String topic, byte[] key, byte[] val) {
            await new SendAwaitable(producer, topic, key, val);
        }

        public async Task Close() { await new CloseAwaitable(producer);}

        public AckDelegate OnAck;
    }

    internal static class Native {
        [DllImport("../../target/debug/libkarst")]
        internal static extern void init_log();

        [DllImport("../../target/debug/libkarst")]
        internal static extern void create_producer(
            Action continuation, 
            [MarshalAs(UnmanagedType.LPUTF8Str)] String bootstrap, 
            AsyncCreateProducerDelegate callback
        );

        [DllImport("../../target/debug/libkarst")]
        internal unsafe static extern void send(IntPtr producer, [MarshalAs(UnmanagedType.LPUTF8Str)] String topic, 
            byte* key, int keyLen, byte* val, int valLen, Action continuation, AsyncDelegate callback);

        [DllImport("../../target/debug/libkarst")]
        internal static extern void close(IntPtr producer, Action continuation, AsyncDelegate callback);
    }
}
