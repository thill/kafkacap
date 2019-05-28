/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.handler;

import io.thill.kafkacap.core.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

/**
 * Encapsulates the logic of a {@link io.thill.kafkacap.core.dedup.Deduplicator}. It provides concurrency for calls to {@link
 * io.thill.kafkacap.core.dedup.outbound.RecordSender#send(int, Object, Object, Headers)}, dispatches calls to an underlying {@link
 * io.thill.kafkacap.core.dedup.strategy.DedupStrategy}, and forwards appropriate records to a {@link io.thill.kafkacap.core.dedup.outbound.KafkaRecordSender}
 *
 * @param <K> The kafka record key type
 * @param <V> The kafka record value type
 * @author Eric Thill
 */
public interface RecordHandler<K, V> extends AutoCloseable {

  void start();

  /**
   * Handle the given record
   *
   * @param record
   * @param topicIdx
   */
  void handle(ConsumerRecord<K, V> record, int topicIdx);

  /**
   * Flush outbound buffers
   */
  void flush();

  /**
   * Check cached messages. This is called separate from {@link RecordHandler#handle(ConsumerRecord, int)} so it can be throttled to not impact performance of
   * the normal path.
   *
   * @param partition
   */
  void checkCache(int partition);

  /**
   * Callback when partitions are reassigned. This should be passed to the underlying {@link io.thill.kafkacap.core.dedup.strategy.DedupStrategy}.
   *
   * @param assignment
   */
  void assigned(Assignment<K, V> assignment);

  /**
   * Callback when partitions are unassigned. This should be passed to the underlying {@link io.thill.kafkacap.core.dedup.strategy.DedupStrategy}.
   */
  void revoked();
}
