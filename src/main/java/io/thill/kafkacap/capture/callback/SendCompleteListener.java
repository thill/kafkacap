package io.thill.kafkacap.capture.callback;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface SendCompleteListener {
  void onSendComplete(ProducerRecord<byte[], byte[]> record, long enqueueTime);
}
