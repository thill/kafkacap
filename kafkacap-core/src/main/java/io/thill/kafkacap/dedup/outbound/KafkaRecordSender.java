/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.outbound;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.Properties;

/**
 * Implementation of a {@link RecordSender} that encapsulates a {@link KafkaProducer}
 *
 * @param <K> The record key type
 * @param <V> The record value type
 * @author Eric Thill
 */
public class KafkaRecordSender<K, V> implements RecordSender<K, V> {

  private final Properties producerProperties;
  private final String topic;
  private KafkaProducer<K, V> producer;

  /**
   * KafkaRecordSender Constructor
   *
   * @param producerProperties The properties used to instantiate a {@link KafkaProducer}
   * @param topic              The outbound kafka topic
   */
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
