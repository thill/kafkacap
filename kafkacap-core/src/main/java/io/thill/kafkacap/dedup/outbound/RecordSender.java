/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.outbound;

import org.apache.kafka.common.header.Headers;

public interface RecordSender<K, V> extends AutoCloseable {
  void open();
  void close();
  void send(int partition, K key, V value, Headers headers);
}
