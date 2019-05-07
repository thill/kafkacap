package io.thill.kafkacap.dedup.outbound;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.Properties;

public class KafkaRecordSender<K, V> implements RecordSender<K, V> {

  private final Properties producerProperties;
  private final String topic;
  private KafkaProducer<K, V> producer;

  public KafkaRecordSender(Properties producerProperties, String topic) {
    this.producerProperties = producerProperties;
    this.topic = topic;
  }

  @Override
  public void open() {
    producer = new KafkaProducer<>(producerProperties);
  }

  @Override
  public void close() {
    if(producer != null) {
      producer.close();
      producer = null;
    }
  }

  @Override
  public void send(int partition, K key, V value, Headers headers) {
    producer.send(new ProducerRecord<>(topic, partition, null, key, value, headers));
  }

  @Override
  public String toString() {
    return "KafkaRecordSender{" +
            "topic='" + topic + '\'' +
            '}';
  }
}
