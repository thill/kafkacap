/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.strategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.zookeeper.Testable;

/**
 * TestableMultiProducerDedupStrategy
 *
 * @author Eric Thill
 */
public class TestableMultiProducerDedupStrategy extends MultiProducerDedupStrategy<Long, String> {

  public TestableMultiProducerDedupStrategy() {
    super(key -> new TestableSequencedDedupStrategy(), 20);
  }

  @Override
  protected Object parseProducerKey(ConsumerRecord<Long, String> record) {
    return record.value().split(",")[0];
  }

  @Override
  public void populateHeaders(ConsumerRecord<Long, String> inboundRecord, RecordHeaders outboundHeaders) {

  }
}
