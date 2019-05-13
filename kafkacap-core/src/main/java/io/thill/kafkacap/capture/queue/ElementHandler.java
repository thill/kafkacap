/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.queue;

/**
 * A functional interface for handling records from a {@link CaptureQueue}
 *
 * @author Eric Thill
 */
@FunctionalInterface
public interface ElementHandler {
  /**
   * Called to handle a single record polled from the head of the {@link CaptureQueue}
   *
   * @param payload     The polled payload
   * @param enqueueTime The timestamp representing when this payload was first added to the queue
   */
  void handle(byte[] payload, long enqueueTime);
}
