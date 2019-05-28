/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.cache;

import io.thill.kafkacap.core.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Manages all {@link RecordCache}'s for a {@link io.thill.kafkacap.core.dedup.Deduplicator}
 *
 * @author Eric Thill
 */
public class RecordCacheManager<K, V> implements AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final RecordCacheFactory<K, V> recordCacheFactory;
  private PartitionContext[] partitionContexts;

  public RecordCacheManager(RecordCacheFactory<K, V> recordCacheFactory) {
    this.recordCacheFactory = recordCacheFactory;
  }

  @Override
  public void close() {
    partitionContexts = null;
  }

  /**
   * Add a record to the {@link RecordCache} for the given partition+topicIdx
   *
   * @param partition The partition
   * @param topicIdx  The topicIdx
   * @param record    The record
   */
  public void add(int partition, int topicIdx, ConsumerRecord<K, V> record) {
    partitionContexts[partition].add(topicIdx, record);
  }

  /**
   * Check if the {@link RecordCache} is empty for the given partition
   *
   * @param partition The partition
   * @return Empty
   */
  public boolean isEmpty(int partition) {
    return partitionContexts[partition].isEmpty();
  }

  /**
   * Check if the {@link RecordCache} is empty for the given partition+topicIdx
   *
   * @param partition The partition
   * @param topicIdx  The topicIdx
   * @return Empty
   */
  public boolean isEmpty(int partition, int topicIdx) {
    return partitionContexts[partition].isEmpty(topicIdx);
  }

  /**
   * Peek at the first record in the {@link RecordCache} for the given partition+topicIdx
   *
   * @param partition The partition
   * @param topicIdx  The topicIdx
   * @return The first record
   */
  public ConsumerRecord<K, V> peek(int partition, int topicIdx) {
    return partitionContexts[partition].peek(topicIdx);
  }

  /**
   * Poll at the first record in the {@link RecordCache} for the given partition+topicIdx
   *
   * @param partition The partition
   * @param topicIdx  The topicIdx
   * @return The polled record
   */
  public ConsumerRecord<K, V> poll(int partition, int topicIdx) {
    return partitionContexts[partition].poll(topicIdx);
  }

  /**
   * Check the size of the {@link RecordCache} for the given partition+topicIdx
   *
   * @param partition The partition
   * @param topicIdx  The topicIdx
   * @return The size of the {@link RecordCache}
   */
  public int size(int partition, int topicIdx) {
    return partitionContexts[partition].size(topicIdx);
  }

  /**
   * Get the record at given index from the {@link RecordCache} for the given partition+topicIdx
   *
   * @param partition The partition
   * @param topicIdx  The topicIdx
   * @param recordIdx The recordIdx
   * @return The record
   */
  public ConsumerRecord<K, V> get(int partition, int topicIdx, int recordIdx) {
    return partitionContexts[partition].get(topicIdx, recordIdx);
  }

  /**
   * Remove and return the record at the given index from the {@link RecordCache} for the given partition+topicIdx
   *
   * @param partition The partition
   * @param topicIdx  The topicIdx
   * @param recordIdx The recordIdx
   * @return The removed record
   */
  public ConsumerRecord<K, V> remove(int partition, int topicIdx, int recordIdx) {
    return partitionContexts[partition].remove(topicIdx, recordIdx);
  }

  /**
   * Check if the underlying {@link RecordCache} supports unordered records.  See {@link RecordCache#supportsUnordered()}
   *
   * @return true if unordered records are supported, false otherwise
   */
  public boolean supportsUnordered() {
    return recordCacheFactory.create().supportsUnordered();
  }

  /**
   * Called when new partitions are assigned. This should construct new {@link RecordCache}s with a clean state.
   *
   * @param assignment The record assignment
   */
  public void assigned(Assignment<K, V> assignment) {
    logger.debug("Creating contexts for partitions {}", assignment.getPartitions());
    if(assignment.getPartitions().size() == 0) {
      partitionContexts = new PartitionContext[0];
    } else {
      partitionContexts = new PartitionContext[Collections.max(assignment.getPartitions()) + 1];
      for(Integer partition : assignment.getPartitions()) {
        partitionContexts[partition] = new PartitionContext(partition, assignment.getNumTopics(), recordCacheFactory);
      }
    }
  }

  /**
   * Called when partitions are revoked. This should close all existing underlying {@link RecordCache}s
   */
  public void revoked() {
    logger.debug("Clearing State");
    if(partitionContexts != null) {
      for(PartitionContext<K, V> partitionContext : partitionContexts)
        partitionContext.close();
    }
    partitionContexts = null;
  }

  private static class PartitionContext<K, V> {
    private final List<RecordCache<K, V>> topicCaches = new ArrayList<>();

    public PartitionContext(int partition, int numTopics, RecordCacheFactory<K, V> recordCacheFactory) {
      for(int i = 0; i < numTopics; i++) {
        RecordCache<K, V> rc = recordCacheFactory.create();
        rc.start(partition, numTopics);
        topicCaches.add(rc);
      }
    }

    public void close() {
      for(RecordCache<K, V> rc : topicCaches)
        rc.close();
    }

    public boolean isEmpty() {
      for(int i = 0; i < topicCaches.size(); i++) {
        if(!topicCaches.get(i).isEmpty())
          return false;
      }
      return true;
    }

    public boolean isEmpty(int topicIdx) {
      return topicCaches.get(topicIdx).isEmpty();
    }

    public int size(int topicIdx) {
      return topicCaches.get(topicIdx).size();
    }

    public void add(int topicIdx, ConsumerRecord<K, V> record) {
      topicCaches.get(topicIdx).add(record);
    }

    public ConsumerRecord<K, V> peek(int topicIdx) {
      return topicCaches.get(topicIdx).peek();
    }

    public ConsumerRecord<K, V> poll(int topicIdx) {
      return topicCaches.get(topicIdx).poll();
    }

    public ConsumerRecord<K, V> get(int topicIdx, int recordIdx) {
      return topicCaches.get(topicIdx).get(recordIdx);
    }

    public ConsumerRecord<K, V> remove(int topicIdx, int recordIdx) {
      return topicCaches.get(topicIdx).remove(recordIdx);
    }
  }
}
