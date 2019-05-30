/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.strategy;

import io.thill.kafkacap.core.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * A strategy that ultimately decides how messages are deduplicated
 *
 * @param <K> kafka record key type
 * @param <V> kafka record value type
 * @author Eric Thill
 */
public interface DedupStrategy<K, V> {

  /**
   * Check if the given record should be sent, dropped, or added to a queue that will be checked again soon.
   *
   * @param record The record to check
   * @return sent, dropped, or checked again soon
   */
  DedupResult check(ConsumerRecord<K, V> record);

  /**
   * Optionally populate additional outbound headers
   *
   * @param inboundRecord
   * @param outboundHeaders
   */
  default void populateHeaders(ConsumerRecord<K, V> inboundRecord, RecordHeaders outboundHeaders) {
  }

  /**
   * Callback when partitions are reassigned
   *
   * @param assignment
   */
  void assigned(Assignment<K, V> assignment);

  /**
   * Callback when partitions are unassigned
   */
  void revoked();

}
