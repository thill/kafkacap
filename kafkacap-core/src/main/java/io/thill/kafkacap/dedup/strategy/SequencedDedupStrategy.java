/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.strategy;

import io.thill.kafkacap.dedup.assignment.Assignment;
import io.thill.kafkacap.util.constant.RecordHeaderKeys;
import io.thill.kafkacap.util.io.BitUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An abstract implementation of a {@link DedupStrategy} that assumes each record contains an incrementing unique sequence number
 *
 * @param <K> kafka record key type
 * @param <V> kafka record value type
 */
public abstract class SequencedDedupStrategy<K, V> implements DedupStrategy<K, V> {

  private static final long DEFAULT_SEQUENCE_GAP_MILLIS = Duration.ofSeconds(10).toMillis();

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final long sequenceGapTimeoutMillis;
  private PartitionContext[] partitionContexts;
  private int numTopics;

  /**
   * SequencedDedupStrategy Constructor that uses a 10 second gap timeout
   */
  public SequencedDedupStrategy() {
    this(DEFAULT_SEQUENCE_GAP_MILLIS);
  }

  /**
   * SequencedDedupStrategy Constructor that uses the given gap timeout. If all inbound records miss a message, it will be skipped immediately. A gap timeout
   * will only be used when a Capture Device is down and a gap is detected across all remaining streams.
   *
   * @param sequenceGapTimeoutMillis The time that needs to elapsed with a missing message before forcing processing to continue
   */
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
      ctx.nextSequence = sequence + parseSequenceDelta(record);
      ctx.startGapTimestamp = 0;
      ctx.nextSequenceNull = false;
      return DedupResult.SEND;
    } else if(sequence == ctx.nextSequence) {
      // message is the next expected sequence, send it
      ctx.nextSequence += parseSequenceDelta(record);
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
   * Can be overwritten to process payloads that contain multiple sequence numbers
   *
   * @param record
   * @return
   */
  protected long parseSequenceDelta(ConsumerRecord<K, V> record) {
    return 1;
  }

  @Override
  public void populateHeaders(ConsumerRecord<K, V> inboundRecord, RecordHeaders outboundHeaders) {
    final long sequence = parseSequence(inboundRecord);
    final long sequenceDelta = parseSequenceDelta(inboundRecord);
    outboundHeaders.add(RecordHeaderKeys.HEADER_KEY_DEDUP_SEQUENCE, BitUtil.longToBytes(sequence));
    outboundHeaders.add(RecordHeaderKeys.HEADER_KEY_DEDUP_NUM_SEQUENCES, BitUtil.longToBytes(sequenceDelta));
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
  public void assigned(Assignment<K, V> assignment) {
    logger.info("Assigned: {}", assignment.getPartitions());
    this.numTopics = assignment.getNumTopics();
    if(assignment.getPartitions().size() == 0) {
      partitionContexts = new PartitionContext[0];
    } else {
      partitionContexts = new PartitionContext[Collections.max(assignment.getPartitions()) + 1];
      for(Integer partition : assignment.getPartitions()) {
        partitionContexts[partition] = new PartitionContext();
        final ConsumerRecord<K, V> lastRecord = assignment.getLastOutboundRecord(partition);
        final Header sequenceHeader = lastRecord != null ? lastRecord.headers().lastHeader(RecordHeaderKeys.HEADER_KEY_DEDUP_SEQUENCE) : null;
        if(sequenceHeader != null) {
          final Header numSequencesHeader = lastRecord.headers().lastHeader(RecordHeaderKeys.HEADER_KEY_DEDUP_NUM_SEQUENCES);
          final long nextSequence = BitUtil.bytesToLong(sequenceHeader.value()) + BitUtil.bytesToLong(numSequencesHeader.value());
          logger.info("Partition {} recovered next sequence: {}", partition, nextSequence);
          partitionContexts[partition].nextSequence = nextSequence;
          partitionContexts[partition].nextSequenceNull = false;
        } else {
          logger.info("No sequence to recover");
        }
      }
    }
  }

  @Override
  public void revoked() {
    logger.info("Revoked");
    this.numTopics = 0;
    this.partitionContexts = null;
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
