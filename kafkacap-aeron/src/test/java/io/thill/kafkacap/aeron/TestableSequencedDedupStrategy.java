/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.aeron;

import io.thill.kafkacap.core.dedup.strategy.SequencedDedupStrategy;
import io.thill.kafkacap.core.util.io.BitUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TestableSequencedDedupStrategy extends SequencedDedupStrategy<byte[], byte[]> {

  public TestableSequencedDedupStrategy(String name) {
    super(false, 5000, name);
  }

  @Override
  protected long parseSequence(ConsumerRecord<byte[], byte[]> record) {
    return BitUtil.bytesToLong(record.value());
  }

  @Override
  protected void onSequenceGap(int partition, long fromSequence, long toSequence) {

  }

}