/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.callback;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

/**
 * A functional interface meant for callbacks to monitor records as they are deduplicated and sent.
 *
 * @param <K> The inbound record key type
 * @param <V> The inbound record value type
 * @author Eric Thill
 */
@FunctionalInterface
public interface DedupCompleteListener<K, V> {
  /**
   * Called after the call to {@link io.thill.kafkacap.dedup.outbound.RecordSender#send(int, Object, Object, Headers)} has completed.
   *
   * @param consumerRecord  The inbound record that generated the outbound payload
   * @param producerHeaders The generated outbound kafka headers
   */
  void onDedupComplete(ConsumerRecord<K, V> consumerRecord, Headers producerHeaders);
}
