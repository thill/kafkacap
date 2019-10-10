package io.thill.kafkacap.core.dedup.strategy;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteOrder;

public class TestBinarySequencedDedupStrategy {

  @Test
  public void testKeyWithZeroOffset() {
    BinarySequencedKeyDedupStrategy strategy = new BinarySequencedKeyDedupStrategy(true, 10000, "", 0, ByteOrder.LITTLE_ENDIAN);
    byte[] bytes = new byte[16];
    MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
    buffer.putLong(0, 1234567890123L, ByteOrder.LITTLE_ENDIAN);
    long parsed = strategy.parseSequence(new ConsumerRecord<>("topic", 0, 0, bytes, new byte[0]));
    Assert.assertEquals(1234567890123L, parsed);
  }

  @Test
  public void testValueWithNonZeroOffset() {
    BinarySequencedValueDedupStrategy strategy = new BinarySequencedValueDedupStrategy(true, 10000, "", 8, ByteOrder.LITTLE_ENDIAN);
    byte[] bytes = new byte[16];
    MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
    buffer.putLong(8, 1234567890123L, ByteOrder.LITTLE_ENDIAN);
    long parsed = strategy.parseSequence(new ConsumerRecord<>("topic", 0, 0, new byte[0], bytes));
    Assert.assertEquals(1234567890123L, parsed);
  }

}
