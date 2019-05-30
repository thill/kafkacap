/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.aeron;

import io.aeron.protocol.DataHeaderFlyweight;
import io.thill.kafkacap.core.dedup.assignment.Assignment;
import io.thill.kafkacap.core.dedup.strategy.DedupResult;
import io.thill.kafkacap.core.util.io.BitUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * TestAeronMultiProducerDedupStreategy
 *
 * @author Eric Thill
 */
public class TestAeronDedupStreategy {

  @Test
  public void testTwoProducers() {
    final AeronDedupStrategy dedupStrategy = new AeronDedupStrategy(name -> new TestableSequencedDedupStrategy(name), 2000);
    dedupStrategy.assigned(new Assignment<>(Arrays.asList(0), 2, Collections.emptyMap(), Collections.emptyMap()));

    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(1000, 10000L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(2000, 100L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(1000, 10001L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(2000, 101L)));
    Assert.assertEquals(DedupResult.DROP, dedupStrategy.check(record(2000, 101L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(2000, 102L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(1000, 10002L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(1000, 10003L)));
    Assert.assertEquals(DedupResult.CACHE, dedupStrategy.check(record(1000, 10005L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(2000, 103L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(2000, 104L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(1000, 10004L)));
    Assert.assertEquals(DedupResult.SEND, dedupStrategy.check(record(1000, 10005L)));
  }

  private static ConsumerRecord<byte[], byte[]> record(int sessionId, long sequence) {
    byte[] key = new byte[32];
    DataHeaderFlyweight header = new DataHeaderFlyweight();
    header.wrap(key);
    header.sessionId(sessionId);
    return new ConsumerRecord<>("topic", 0, System.currentTimeMillis(), key, BitUtil.longToBytes(sequence));
  }
}
