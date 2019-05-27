/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.cache;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * A {@link RecordCache} implementation that stores all messages in memory and supports size(), get(index), and remove(index).
 *
 * @author Eric Thill
 */
public class MemoryRecordCache<K, V> implements RecordCache<K, V> {

  public static <K, V> RecordCacheFactory<K, V> factory() {
    return MemoryRecordCache::new;
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final LinkedList<ConsumerRecord<K, V>> records = new LinkedList<>();

  @Override
  public void start(int partition, int topicIdx) {
    logger.info("{} created for partition={} topicIdx={}", getClass().getSimpleName(), partition, topicIdx);
  }

  @Override
  public void close() {
    records.clear();
  }

  @Override
  public void add(ConsumerRecord<K, V> record) {
    records.add(record);
  }

  @Override
  public boolean isEmpty() {
    return records.isEmpty();
  }

  @Override
  public ConsumerRecord<K, V> peek() {
    return records.peek();
  }

  @Override
  public ConsumerRecord<K, V> poll() {
    return records.poll();
  }

  @Override
  public int size() {
    return records.size();
  }

  @Override
  public ConsumerRecord<K, V> get(int index) {
    return records.get(index);
  }

  @Override
  public ConsumerRecord<K, V> remove(int index) {
    return records.remove(index);
  }

  @Override
  public boolean supportsUnordered() {
    return true;
  }
}
