/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.capture.populator;

import java.time.Clock;
import io.thill.kafkacap.core.util.constant.RecordHeaderKeys;
import io.thill.kafkacap.core.util.io.BitUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Default implementation of a {@link RecordPopulator} that copies the inbound byte[] payload to the outbound {@link ProducerRecord} value.
 *
 * @param <K> The {@link ProducerRecord} key time.
 * @author Eric Thill
 */
public class DefaultRecordPopulator<K> implements RecordPopulator<K, byte[]> {

  private final String topic;
  private final int partition;
  private final Clock clock;

  /**
   * DefaultRecordPopulator constructor
   *
   * @param topic     The topic to apply to all populated records
   * @param partition The partition to apply to all populated records
   * @param clock     The clock to use for header timestamps
   */
  public DefaultRecordPopulator(String topic, int partition, Clock clock) {
    this.topic = topic;
    this.partition = partition;
    this.clock = clock;
  }

  @Override
  public ProducerRecord<K, byte[]> populate(byte[] payload, long enqueueTime) {
    final K key = key(payload, enqueueTime);
    final byte[] value = value(payload, enqueueTime);
    final RecordHeaders headers = headers(payload, enqueueTime);
    return new ProducerRecord<>(topic, partition, key, value, headers);
  }

  protected RecordHeaders headers(byte[] payload, long enqueueTime) {
    RecordHeaders headers = new RecordHeaders();
    headers.add(RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME, BitUtil.longToBytes(enqueueTime));
    headers.add(RecordHeaderKeys.HEADER_KEY_CAPTURE_SEND_TIME, BitUtil.longToBytes(clock.millis()));
    return headers;
  }

  protected K key(byte[] payload, long enqueueTime) {
    return null;
  }

  protected byte[] value(byte[] payload, long enqueueTime) {
    return payload;
  }

}
