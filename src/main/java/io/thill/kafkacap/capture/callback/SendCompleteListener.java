package io.thill.kafkacap.capture.callback;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface SendCompleteListener<K, V> {
  void onSendComplete(ProducerRecord<K, V> record, long enqueueTime);
}
