/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.queue;

import io.thill.kafkacap.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface DedupQueue<K, V> extends AutoCloseable {

  void start();

  void add(int partition, int topicIdx, ConsumerRecord<K, V> record);

  boolean isEmpty(int partition);

  boolean isEmpty(int partition, int topicIdx);

  ConsumerRecord<K, V> peek(int partition, int topicIdx);

  ConsumerRecord<K, V> poll(int partition, int topicIdx);

  void assigned(Assignment<K, V> assignment);

  void revoked();

}
