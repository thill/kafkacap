/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.queue;

@FunctionalInterface
public interface ElementHandler {
  void handle(byte[] payload, long enqueueTime);
}
