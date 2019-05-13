/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.queue;

import io.thill.kafkacap.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Keeps "future" records in FIFO order per topicIdx+partition
 *
 * @param <K> The record key type
 * @param <V> The record value type
 * @author Eric Thill
 */
public interface DedupQueue<K, V> extends AutoCloseable {

  /**
   * Start the underlying queue
   */
  void start();

  /**
   * Add a record to the topicIdx+partition queue
   *
   * @param partition The partition of the record
   * @param topicIdx  The topicIdx of the record
   * @param record    The record to add
   */
  void add(int partition, int topicIdx, ConsumerRecord<K, V> record);

  /**
   * Check if all queues for a given partition are empty
   *
   * @param partition The partition to check
   * @return true if empty, false otherwise
   */
  boolean isEmpty(int partition);

  /**
   * Check if the queue for the given partition+topicIdx is empty
   *
   * @param partition The partition to check
   * @param topicIdx  The topicIdx to check
   * @return true if empty, false otherwise
   */
  boolean isEmpty(int partition, int topicIdx);

  /**
   * Peek at the first record for the given partition+topicIdx queue
   *
   * @param partition The partition
   * @param topicIdx  The topicIdx
   * @return The record at the front of the queue, null if empty
   */
  ConsumerRecord<K, V> peek(int partition, int topicIdx);

  /**
   * Poll the first record from the given partition+topicIdx queue
   *
   * @param partition The partition
   * @param topicIdx  The topicIdx
   * @return The record polled from the front of the queue, null if empty
   */
  ConsumerRecord<K, V> poll(int partition, int topicIdx);

  /**
   * Called when partitions are assigned
   *
   * @param assignment The partition assignment
   */
  void assigned(Assignment<K, V> assignment);

  /**
   * Called when all partitions are revoked
   */
  void revoked();

}
