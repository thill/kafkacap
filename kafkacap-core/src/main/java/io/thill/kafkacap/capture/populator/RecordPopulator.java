package io.thill.kafkacap.capture.populator;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface RecordPopulator<K, V> {
  ProducerRecord<K, V> populate(byte[] payload, long enqueueTime);
}
