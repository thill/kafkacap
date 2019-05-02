package io.thill.kafkacap.dedup.strategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class SequencedDedupStrategy<K, V> implements DedupStrategy<K, V> {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final long sequenceGapTimeoutMillis;
  private PartitionContext[] partitionContexts;
  private int numTopics;

  public SequencedDedupStrategy(long sequenceGapTimeoutMillis) {
    this.sequenceGapTimeoutMillis = sequenceGapTimeoutMillis;
  }

  @Override
  public DedupResult check(ConsumerRecord<K, V> record) {
    final long sequence = parseSequence(record);
    final PartitionContext ctx = partitionContexts[record.partition()];

    if(ctx.nextSequenceNull) {
      // first sequence ever received for this topic, send it
      logger.info("First Sequence for Partition {}: {}", record.partition(), sequence);
      ctx.nextSequence = sequence + 1;
      ctx.startGapTimestamp = 0;
      ctx.nextSequenceNull = false;
      return DedupResult.SEND;
    } else if(sequence == ctx.nextSequence) {
      // message is the next expected sequence, send it
      ctx.nextSequence++;
      ctx.startGapTimestamp = 0;
      return DedupResult.SEND;
    } else if(Long.compareUnsigned(sequence, ctx.nextSequence) < 0) {
      // message is a prior sequence, drop it
      return DedupResult.DROP;
    } else if(ctx.startGapTimestamp != 0 && System.currentTimeMillis() > ctx.startGapTimestamp + sequenceGapTimeoutMillis) {
      // sequence gap timeout reached: set next sequence to next available sequence, which will be processed the next call to this method for the given partition
      logger.error("Sequence Gap Detected on Partition {}. Missing sequences {} through {}.", record.partition(), ctx.nextSequence, ctx.nextAvailableSequence - 1);
      onSequenceGap(record.partition(), ctx.nextSequence, ctx.nextAvailableSequence - 1);
      ctx.nextSequence = ctx.nextAvailableSequence;
      ctx.nextAvailableSequence = -1;
      ctx.topicsAtNextAvailableSequence.clear();
      ctx.startGapTimestamp = 0;
      return DedupResult.QUEUE;
    } else {
      // message is a future sequence, add to queue
      if(Long.compareUnsigned(sequence, ctx.nextAvailableSequence) < 0) {
        // message is not the next sequence, but represents the next available sequence to be processed: reset the gap timeout and set the nextAvailableSequence
        // note that this INFO might not be logged if this TopicPartition is behind another TopicPartition, but it does provide valuable traceability for sequence gap timeouts
        // also note that the "from" sequence in this INFO might not be accurate for the same reason. It may be missing more sequences than what is logged.
        logger.info("TopicParition {}-{} is missing sequences {} through {}", record.topic(), record.partition(), ctx.nextSequence, sequence - 1);
        ctx.startGapTimestamp = System.currentTimeMillis();
        ctx.nextAvailableSequence = sequence;
        ctx.topicsAtNextAvailableSequence.add(record.topic());
      } else if(sequence == ctx.nextAvailableSequence) {
        ctx.topicsAtNextAvailableSequence.add(record.topic());
        if(ctx.topicsAtNextAvailableSequence.size() == numTopics) {
          // all topics are missing the same sequences, gap timeout is pointless, send it now and adjust context
          logger.info("All topics for partition {} are missing sequences {} through {}", record.partition(), ctx.nextSequence, ctx.nextAvailableSequence - 1);
          onSequenceGap(record.partition(), ctx.nextSequence, ctx.nextAvailableSequence - 1);
          ctx.nextSequence = ctx.nextAvailableSequence + 1;
          ctx.nextAvailableSequence = -1;
          ctx.topicsAtNextAvailableSequence.clear();
          ctx.startGapTimestamp = 0;
          return DedupResult.SEND;
        }
      }
      return DedupResult.QUEUE;
    }
  }

  /**
   * Parse the sequence from the record
   *
   * @param record The record to parse
   * @return The parsed sequence
   */
  protected abstract long parseSequence(ConsumerRecord<K, V> record);

  /**
   * Any special logic the implementing class may want to run on a sequence gap. Useful for alerting.
   *
   * @param partition    The partition
   * @param fromSequence The first missing sequence (inclusive)
   * @param toSequence   The last missing sequence (inclusive)
   */
  protected abstract void onSequenceGap(int partition, long fromSequence, long toSequence);

  @Override
  public final void assigned(Set<Integer> partitions, int numTopics) {
    logger.info("Assigned: {}", partitions);
    this.numTopics = numTopics;
    if(partitions.size() == 0) {
      partitionContexts = new PartitionContext[0];
    } else {
      partitionContexts = new PartitionContext[Collections.max(partitions) + 1];
      for(Integer partition : partitions) {
        partitionContexts[partition] = new PartitionContext();
      }
    }
    onAssigned(partitions, numTopics);
  }

  protected abstract void onAssigned(Set<Integer> partitions, int numTopics);

  @Override
  public final void revoked(Set<Integer> partitions, int numTopics) {
    logger.info("Revoked: {}", partitions);
    onRevoked(partitions, numTopics);
    this.numTopics = 0;
    this.partitionContexts = null;
  }

  protected abstract void onRevoked(Set<Integer> partitions, int numTopics);

  /**
   * Can be used during implementing class's recovery logic during the call to onAssigned
   *
   * @param partition
   * @param nextSequence
   */
  protected void setNextSequence(int partition, long nextSequence) {
    partitionContexts[partition].nextSequence = nextSequence;
    partitionContexts[partition].nextSequenceNull = false;
  }

  private static class PartitionContext {
    // variables used for normal flow
    private boolean nextSequenceNull = true;
    private long nextSequence;

    // variables used when messages are missing from a TopicPartition
    private final Set<String> topicsAtNextAvailableSequence = new HashSet<>();
    private long nextAvailableSequence = -1;
    private long startGapTimestamp;
  }
}
