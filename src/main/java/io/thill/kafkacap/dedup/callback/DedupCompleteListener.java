package io.thill.kafkacap.dedup.callback;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

public interface DedupCompleteListener<K, V> {
  void onDedupComplete(ConsumerRecord<K, V> consumerRecord, Headers producerHeaders);
}
