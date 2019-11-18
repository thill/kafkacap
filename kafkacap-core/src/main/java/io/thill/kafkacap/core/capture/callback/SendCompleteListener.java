/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.capture.callback;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Callback after a {@link org.apache.kafka.clients.producer.KafkaProducer#send(ProducerRecord)} call has completed for a particular {@link ProducerRecord}
 *
 * @param <K> {@link ProducerRecord} key type
 * @param <V> {@link ProducerRecord} value type
 * @author Eric Thill
 */
@FunctionalInterface
public interface SendCompleteListener<K, V> {
  /**
   * @param record      The record that was sent
   * @param enqueueTime The timestamp from the underlying {@link java.time.Clock} representing when this message was added to the {@link
   *                    io.thill.kafkacap.core.capture.queue.CaptureQueue}
   */
  void onSendComplete(ProducerRecord<K, V> record, long enqueueTime);
}
