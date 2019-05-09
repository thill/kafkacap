package io.thill.kafkacap.capture.queue;

public interface CaptureQueue extends AutoCloseable {
  void close();
  void add(byte[] payload, long enqueueTime);
  void add(byte[] payload, int payloadOffset, int payloadLength, long enqueueTime);
  boolean poll(ElementHandler handler);
}
