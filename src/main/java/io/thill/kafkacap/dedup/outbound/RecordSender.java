package io.thill.kafkacap.dedup.outbound;

import org.apache.kafka.common.header.Headers;

public interface RecordSender<K, V> extends AutoCloseable {
  void send(int partition, K key, V value, Headers headers);
}
