/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.populator;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Functional Interface to populate a {@link ProducerRecord} from the given inbound payload
 *
 * @author Eric Thill
 */
@FunctionalInterface
public interface RecordPopulator<K, V> {
  /**
   * pPopulate a {@link ProducerRecord} from the given inbound payload
   *
   * @param payload     The inbound payload
   * @param enqueueTime The timestamp representing the time this payload was first added to the queue
   * @return The populated {@link ProducerRecord}
   */
  ProducerRecord<K, V> populate(byte[] payload, long enqueueTime);
}
