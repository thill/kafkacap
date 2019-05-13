/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.outbound;

import org.apache.kafka.common.header.Headers;

/**
 * Sends the outbound kafka records. Usually will be a {@link KafkaRecordSender}, except for tests.
 *
 * @param <K> The {@link org.apache.kafka.clients.producer.ProducerRecord} key type
 * @param <V> The {@link org.apache.kafka.clients.producer.ProducerRecord} value type
 * @author Eric Thill
 */
public interface RecordSender<K, V> extends AutoCloseable {
  /**
   * Open a new instance of underlying {@link org.apache.kafka.clients.producer.KafkaProducer}
   */
  void open();

  /**
   * Close the existing instance of underlying {@link org.apache.kafka.clients.producer.KafkaProducer}
   */
  void close();

  /**
   * Send a record asynchronously using the given values
   *
   * @param partition The outbound partition
   * @param key       The {@link org.apache.kafka.clients.producer.ProducerRecord} key
   * @param value     The {@link org.apache.kafka.clients.producer.ProducerRecord} value
   * @param headers   The {@link org.apache.kafka.clients.producer.ProducerRecord} headers
   */
  void send(int partition, K key, V value, Headers headers);
}
