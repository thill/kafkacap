package io.thill.kafkacap.dedup.strategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;

public interface DedupStrategy<K, V> {

  /**
   * Check if the given record should be sent, dropped, or added to a queue that will be checked again soon.
   *
   * @param record The record to check
   * @return sent, dropped, or checked again soon
   */
  DedupResult check(ConsumerRecord<K, V> record);

  /**
   * Callback when partitions are reassigned
   *
   * @param partitions
   * @param numTopics
   */
  void assigned(Collection<Integer> partitions, int numTopics);

  /**
   * Callback when partitions are unassigned
   *
   * @param partitions
   * @param numTopics
   */
  void revoked(Collection<Integer> partitions, int numTopics);

}
