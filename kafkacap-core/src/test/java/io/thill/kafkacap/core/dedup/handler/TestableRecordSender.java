/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.handler;

import io.thill.kafkacap.core.dedup.outbound.RecordSender;
import org.apache.kafka.common.header.Headers;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestableRecordSender implements RecordSender<Long, String> {

  private final BlockingQueue<TestRecord> queue = new LinkedBlockingQueue<>();

  public TestRecord poll() throws InterruptedException {
    return queue.poll(1, TimeUnit.SECONDS);
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }


  @Override
  public void open() {

  }

  @Override
  public void close() {
    queue.clear();
  }

  @Override
  public void send(int partition, Long key, String value, Headers headers) {
    queue.add(new TestRecord(partition, key, value));
  }

  @Override
  public void flush() {

  }

  public static class TestRecord {
    private final int partition;
    private final Long key;
    private final String value;

    public TestRecord(int partition, Long key, String value) {
      this.partition = partition;
      this.key = key;
      this.value = value;
    }

    public int partition() {
      return partition;
    }

    public Long key() {
      return key;
    }

    public String value() {
      return value;
    }
  }
}
