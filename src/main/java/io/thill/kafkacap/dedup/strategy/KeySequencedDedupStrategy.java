package io.thill.kafkacap.dedup.strategy;

import io.thill.kafkacap.util.io.BitUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KeySequencedDedupStrategy extends SequencedDedupStrategy<byte[], byte[]> {

  public KeySequencedDedupStrategy() {

  }

  public KeySequencedDedupStrategy(long sequenceGapTimeoutMillis) {
    super(sequenceGapTimeoutMillis);
  }

  @Override
  protected long parseSequence(ConsumerRecord<byte[], byte[]> record) {
    return BitUtil.bytesToLong(record.key());
  }

  @Override
  protected void onSequenceGap(int partition, long fromSequence, long toSequence) {

  }

}
