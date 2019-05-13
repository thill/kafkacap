/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.queue;

public interface CaptureQueue extends AutoCloseable {
  void close();
  void add(byte[] payload, long enqueueTime);
  void add(byte[] payload, int payloadOffset, int payloadLength, long enqueueTime);
  boolean poll(ElementHandler handler);
}
