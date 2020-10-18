package org.karst;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Karst {
    static {
        try {
            System.loadLibrary("karst_jvm");
        } catch(java.lang.UnsatisfiedLinkError e) {
            System.err.println("Unable to load native library. " + e.getMessage());
            System.exit(0);
        }
    }

    private static native long createProducer(String bootstrap);
    private static native void producerSendString(long producer, String topic, String key, String value);
    private static native void producerClose(long producer);

    public static void main(String[] args) {
        System.out.println("J: creating");
        Long p = createProducer("localhost:9092");
        System.out.println("J: got producer: " + p);
        producerSendString(p, "test1", "k1", "v1");
        System.out.println("Sent message. Closing...");
        producerClose(p);
        System.out.println("Closed");
    }
}