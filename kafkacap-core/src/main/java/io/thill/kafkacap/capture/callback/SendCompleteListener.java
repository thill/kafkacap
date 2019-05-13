/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.callback;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface SendCompleteListener<K, V> {
  void onSendComplete(ProducerRecord<K, V> record, long enqueueTime);
}
