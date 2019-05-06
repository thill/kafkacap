package io.thill.kafkacap.dedup.queue;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;

public interface DedupQueue<K, V> {

  void add(int partition, int topicIdx, ConsumerRecord<K, V> record);

  boolean isEmpty(int partition);

  boolean isEmpty(int partition, int topicIdx);

  ConsumerRecord<K, V> peek(int partition, int topicIdx);

  ConsumerRecord<K, V> poll(int partition, int topicIdx);

  void assigned(Collection<Integer> partitions, int numTopics);

  void revoked(Collection<Integer> partitions, int numTopics);

}
