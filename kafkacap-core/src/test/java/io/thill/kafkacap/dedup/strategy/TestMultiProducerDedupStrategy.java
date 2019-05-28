/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.strategy;

import io.thill.kafkacap.dedup.assignment.Assignment;
import io.thill.kafkacap.util.io.BitUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * TestMultiProducerDedupStrategy
 *
 * @author Eric Thill
 */
public class TestMultiProducerDedupStrategy {

  @Test
  public void testTwoProducers() {
    final TestableMultiProducerDedupStrategy dedupStrategy = new TestableMultiProducerDedupStrategy();
    dedupStrategy.assigned(new Assignment<>(Arrays.asList(0), 2, Collections.emptyMap(), Collections.emptyMap()));

    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P1", 10000L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P2", 100L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P1", 10001L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P2", 101L)));
    Assert.assertEquals(DedupResult.DROP, dedupStrategy.check(record("P2", 101L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P2", 102L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P1", 10002L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P1", 10003L)));
    Assert.assertEquals(DedupResult.CACHE, dedupStrategy.check(record("P1", 10005L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P2", 103L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P2", 104L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P1", 10004L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P1", 10005L)));
  }

  @Test
  public void testTwoProducersRecovery() {
    final TestableMultiProducerDedupStrategy dedupStrategy = new TestableMultiProducerDedupStrategy();

    // assigned at P1=10001 and P2=102
    final Map<Integer, ConsumerRecord<Long, String>> lastOutboundRecords = new LinkedHashMap<>();
    final RecordHeaders headers = new RecordHeaders();
    headers.add("P1-DDSEQ", BitUtil.longToBytes(10001));
    headers.add("P2-DDSEQ", BitUtil.longToBytes(102));
    headers.add("P1-DDNUM", BitUtil.longToBytes(1));
    headers.add("P2-DDNUM", BitUtil.longToBytes(1));
    lastOutboundRecords.put(0, new ConsumerRecord<>("topic", 0, 0, System.currentTimeMillis(),null, 0L, 0, 0, 102L, "P2,102", headers));
    dedupStrategy.assigned(new Assignment<>(Arrays.asList(0), 2, lastOutboundRecords, Collections.emptyMap()));

    Assert.assertEquals(DedupResult.DROP, dedupStrategy.check(record("P1", 10000L)));
    Assert.assertEquals(DedupResult.DROP, dedupStrategy.check(record("P2", 100L)));
    Assert.assertEquals(DedupResult.DROP, dedupStrategy.check(record("P1", 10001L)));
    Assert.assertEquals(DedupResult.DROP, dedupStrategy.check(record("P2", 101L)));
    Assert.assertEquals(DedupResult.DROP, dedupStrategy.check(record("P2", 102L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P1", 10002L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P1", 10003L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P2", 103L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P2", 104L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P1", 10004L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record("P1", 10005L)));
  }

  private static ConsumerRecord<Long, String> record(String producer, long sequence) {
    return new ConsumerRecord<>("topic", 0, System.currentTimeMillis(), sequence, producer + "," + sequence);
  }
}
