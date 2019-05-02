package io.thill.kafkacap.dedup.strategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Set;

public class TestableSequencedDedupStrategy extends SequencedDedupStrategy<String, String> {

  public TestableSequencedDedupStrategy(long sequenceGapTimeoutMillis) {
    super(sequenceGapTimeoutMillis);
  }

  @Override
  protected long parseSequence(ConsumerRecord<String, String> record) {
    return Long.parseLong(record.key());
  }

  @Override
  protected void onAssigned(Set<Integer> partitions, int numTopics) {

  }

  @Override
  protected void onRevoked(Set<Integer> partitions, int numTopics) {

  }
}