/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.handler;

import io.thill.kafkacap.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SynchronizedRecordHandler<K, V> implements RecordHandler<K, V> {

  private final RecordHandler<K, V> underlyingRecordHandler;

  public SynchronizedRecordHandler(RecordHandler<K, V> underlyingRecordHandler) {
    this.underlyingRecordHandler = underlyingRecordHandler;
  }

  @Override
  public synchronized void start() {
    underlyingRecordHandler.start();
  }

  @Override
  public synchronized void close() throws Exception {
    underlyingRecordHandler.close();
  }

  @Override
  public synchronized void handle(final ConsumerRecord<K, V> record, final int topicIdx) {
    underlyingRecordHandler.handle(record, topicIdx);
  }

  @Override
  public synchronized void tryDequeue(final int partition) {
    underlyingRecordHandler.tryDequeue(partition);
  }

  @Override
  public synchronized void assigned(final Assignment<K, V> assignment) {
    underlyingRecordHandler.assigned(assignment);
  }

  @Override
  public synchronized void revoked() {
    underlyingRecordHandler.revoked();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
