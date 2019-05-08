package io.thill.kafkacap.dedup.queue;

import io.thill.kafkacap.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface DedupQueue<K, V> {

  void add(int partition, int topicIdx, ConsumerRecord<K, V> record);

  boolean isEmpty(int partition);

  boolean isEmpty(int partition, int topicIdx);

  ConsumerRecord<K, V> peek(int partition, int topicIdx);

  ConsumerRecord<K, V> poll(int partition, int topicIdx);

  void assigned(Assignment<K, V> assignment);

  void revoked();

}
