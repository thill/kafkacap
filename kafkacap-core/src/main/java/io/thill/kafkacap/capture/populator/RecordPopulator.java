/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.populator;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface RecordPopulator<K, V> {
  ProducerRecord<K, V> populate(byte[] payload, long enqueueTime);
}
