/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.strategy;

import io.thill.kafkacap.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

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
  void populateHeaders(ConsumerRecord<K, V> inboundRecord, RecordHeaders outboundHeaders);

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
