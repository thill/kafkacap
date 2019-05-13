/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.queue;

/**
 * A queued used for buffering inbound messages in a {@link io.thill.kafkacap.capture.BufferedPublisher} and {@link io.thill.kafkacap.capture.CaptureDevice}
 *
 * @author Eric Thill
 */
public interface CaptureQueue extends AutoCloseable {
  /**
   * Called to close the queue when it will no longer be used
   */
  void close();

  /**
   * Add an entire single inbound payload to the queue
   *
   * @param payload     The payload to add
   * @param enqueueTime A timestamp representing "now"
   */
  void add(byte[] payload, long enqueueTime);

  /**
   * Add an inbound payload to the queue from a specific offset in a buffer
   *
   * @param payload       The buffer
   * @param payloadOffset The start of the payload
   * @param payloadLength The length of the payload
   * @param enqueueTime   A timestamp representing "now"
   */
  void add(byte[] payload, int payloadOffset, int payloadLength, long enqueueTime);

  /**
   * Poll a record from the queue to the given {@link ElementHandler}
   *
   * @param handler The handler for the polled record
   * @return True if a record was polled/handled, false otherwise
   */
  boolean poll(ElementHandler handler);
}
