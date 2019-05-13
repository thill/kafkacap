/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.queue;

public class TestMemoryDedupQueue extends AbstractDedupQueueTest {

  @Override
  protected DedupQueue<String, String> createQueue() {
    return new MemoryDedupQueue<>();
  }

}
