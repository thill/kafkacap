/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.aeron;

import io.aeron.protocol.DataHeaderFlyweight;
import io.thill.kafkacap.core.capture.populator.DefaultRecordPopulator;
import java.time.Clock;

import java.util.Arrays;

/**
 * {@link io.thill.kafkacap.core.capture.populator.RecordPopulator} implementation that populates the Kafka Key with the Aeron header, and the Kafka value with
 * the Aeron payload.
 *
 * @author Eric Thill
 */
public class AeronRecordPopulator extends DefaultRecordPopulator<byte[]> {

  /**
   * AeronRecordPopulator constructor
   *
   * @param topic     The topic to apply to all populated records
   * @param partition The partition to apply to all populated records
   * @param clock     The clock to use for header timestamps
   */
  public AeronRecordPopulator(String topic, int partition, Clock clock) {
    super(topic, partition, clock);
  }

  @Override
  protected byte[] key(byte[] payload, long enqueueTime) {
    // first 32 bytes is the header
    return Arrays.copyOfRange(payload, 0, DataHeaderFlyweight.HEADER_LENGTH);
  }

  @Override
  protected byte[] value(byte[] payload, long enqueueTime) {
    // all data after header is the value
    return Arrays.copyOfRange(payload, DataHeaderFlyweight.HEADER_LENGTH, payload.length);
  }

}
