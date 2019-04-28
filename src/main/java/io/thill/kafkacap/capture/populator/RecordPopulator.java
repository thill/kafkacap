package io.thill.kafkacap.capture.populator;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface RecordPopulator {
  ProducerRecord<byte[], byte[]> populate(byte[] payload, long enqueueTime);
}
