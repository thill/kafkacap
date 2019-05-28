/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.strategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * TestableMultiProducerDedupStrategy
 *
 * @author Eric Thill
 */
public class TestableMultiProducerDedupStrategy extends MultiProducerDedupStrategy<Long, String> {

  public TestableMultiProducerDedupStrategy() {
    super(key -> new TestableSequencedDedupStrategy(false, 2000, key.toString() + "-"), 5);
  }

  public TestableMultiProducerDedupStrategy(int producerExpireIntervalSeconds) {
    super(key -> new TestableSequencedDedupStrategy(false, 2000, key.toString() + "-"), producerExpireIntervalSeconds);
  }

  @Override
  protected String parseProducerKey(ConsumerRecord<Long, String> record) {
    return record.value().split(",")[0];
  }

  @Override
  public void populateHeaders(ConsumerRecord<Long, String> inboundRecord, RecordHeaders outboundHeaders) {

  }
}
