/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.cache;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Keeps "future" records in FIFO order for a single partition+topicIdx
 *
 * @param <K> The record key type
 * @param <V> The record value type
 * @author Eric Thill
 */
public interface RecordCache<K, V> extends AutoCloseable {

  /**
   * Start the underlying cache
   *
   * @param partition The partition assigned to this record cache
   * @param topicIdx  The topicIdx assigned ot this record cache
   */
  void start(int partition, int topicIdx);

  /**
   * Stop the underlying cache and close any open resources
   */
  void close();

  /**
   * Add a record to the topicIdx+partition queue
   *
   * @param record The record to add
   */
  void add(ConsumerRecord<K, V> record);

  /**
   * Check if there are zero records in the cache
   *
   * @return true if empty, false otherwise
   */
  boolean isEmpty();

  /**
   * Peek at the first record in the cache
   *
   * @return The record at the front of the queue, null if empty
   */
  ConsumerRecord<K, V> peek();

  /**
   * Poll the first record from the cache
   *
   * @return The record polled from the front of the queue, null if empty
   */
  ConsumerRecord<K, V> poll();

  /**
   * Return the number of records in the cache. Only required for unordered record captures.
   *
   * @return the number of records in the cache
   */
  int size();

  /**
   * Return the record in the cache at the given index.  The first added record should be represented by index=0, and the most recently added record should be
   * represented by index=size()-1. Only required for unordered record captures.
   *
   * @param index the index within the cache to lookup
   * @return the record at the given index
   */
  ConsumerRecord<K, V> get(int index);

  /**
   * Remove and return the record in the cache at the given index.  The first added record should be represented by index=0, and the most recently added record
   * should be represented by index=size()-1. Only required for unordered record captures.
   *
   * @param index the index within the cache to lookup
   * @return the record at the given index
   */
  ConsumerRecord<K, V> remove(int index);

  /**
   * Check if this implementation supports unordered records
   *
   * @return true if this implementation supports size(), get(index), and remove(index).  False otherwise.
   */
  boolean supportsUnordered();

}
