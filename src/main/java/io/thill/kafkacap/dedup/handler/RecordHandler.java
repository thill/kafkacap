package io.thill.kafkacap.dedup.handler;

import io.thill.kafkacap.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecordHandler<K, V> {

  /**
   * Handle the given record
   *
   * @param record
   * @param topicIdx
   */
  void handle(ConsumerRecord<K, V> record, int topicIdx);

  /**
   * Attempt to dequeue messages. This is called separate from {@link RecordHandler#handle(ConsumerRecord, int)} so it can be throttled to not impact
   * performance of the normal path.
   *
   * @param partition
   */
  void tryDequeue(int partition);

  /**
   * Callback when partitions are reassigned. This should be passed to the underlying {@link io.thill.kafkacap.dedup.strategy.DedupStrategy}.
   *
   * @param assignment
   */
  void assigned(Assignment<K, V> assignment);

  /**
   * Callback when partitions are unassigned. This should be passed to the underlying {@link io.thill.kafkacap.dedup.strategy.DedupStrategy}.
   */
  void revoked();
}
