package io.thill.kafkacap.dedup.strategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TestableSequencedDedupStrategy extends SequencedDedupStrategy<String, String> {

  private Integer lastGapPartition;
  private Long lastGapFromSequence;
  private Long lastGapToSequence;

  public TestableSequencedDedupStrategy(long sequenceGapTimeoutMillis) {
    super(sequenceGapTimeoutMillis);
  }

  @Override
  protected long parseSequence(ConsumerRecord<String, String> record) {
    return Long.parseLong(record.key());
  }

  @Override
  protected void onSequenceGap(int partition, long fromSequence, long toSequence) {
    lastGapPartition = partition;
    lastGapFromSequence = fromSequence;
    lastGapToSequence = toSequence;
  }

  public Integer getLastGapPartition() {
    return lastGapPartition;
  }

  public Long getLastGapFromSequence() {
    return lastGapFromSequence;
  }

  public Long getLastGapToSequence() {
    return lastGapToSequence;
  }

}