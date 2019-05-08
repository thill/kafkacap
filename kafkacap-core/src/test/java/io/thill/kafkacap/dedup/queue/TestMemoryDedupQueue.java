package io.thill.kafkacap.dedup.queue;

public class TestMemoryDedupQueue extends AbstractDedupQueueTest {

  @Override
  protected DedupQueue<String, String> createQueue() {
    return new MemoryDedupQueue<>();
  }

}
