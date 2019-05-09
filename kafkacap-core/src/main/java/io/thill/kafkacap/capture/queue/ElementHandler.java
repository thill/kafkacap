package io.thill.kafkacap.capture.queue;

@FunctionalInterface
public interface ElementHandler {
  void handle(byte[] payload, long enqueueTime);
}
