package io.thill.kafkacap.dedup.outbound;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.Properties;

public class KafkaRecordSender<K, V> implements RecordSender<K, V> {

  private final KafkaProducer<K, V> producer;
  private final String topic;

  public KafkaRecordSender(Properties producerProperties, String topic) {
    this.producer = new KafkaProducer<>(producerProperties);
    this.topic = topic;
  }

  @Override
  public void send(int partition, K key, V value, Headers headers) {
    producer.send(new ProducerRecord<>(topic, partition, null, key, value));
  }

  @Override
  public void close() {
    producer.close();
  }

  @Override
  public String toString() {
    return "KafkaRecordSender{" +
            "topic='" + topic + '\'' +
            '}';
  }
}
