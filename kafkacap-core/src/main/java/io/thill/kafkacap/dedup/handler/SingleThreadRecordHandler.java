/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.handler;

import io.thill.kafkacap.dedup.assignment.Assignment;
import io.thill.kafkacap.dedup.callback.DedupCompleteListener;
import io.thill.kafkacap.dedup.outbound.RecordSender;
import io.thill.kafkacap.dedup.queue.DedupQueue;
import io.thill.kafkacap.dedup.recovery.PartitionOffsets;
import io.thill.kafkacap.dedup.strategy.DedupResult;
import io.thill.kafkacap.dedup.strategy.DedupStrategy;
import io.thill.kafkacap.util.clock.Clock;
import io.thill.kafkacap.util.constant.RecordHeaderKeys;
import io.thill.kafkacap.util.io.BitUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a {@link RecordHandler} that handles flow between the {@link DedupStrategy}, {@link DedupQueue}, and {@link RecordSender}, but is not
 * thread-safe. It is meant to be encapsulated by a thread-safe implementation.
 *
 * @param <K> The kafka record key type
 * @param <V> The kafka record value type
 * @author Eric Thill
 */
public class SingleThreadRecordHandler<K, V> implements RecordHandler<K, V> {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final DedupStrategy<K, V> dedupStrategy;
  private final DedupQueue<K, V> dedupQueue;
  private final RecordSender<K, V> sender;
  private final Clock clock;
  private final DedupCompleteListener<K, V> dedupCompleteListener;
  private int numTopics;
  private PartitionOffsets offsets;

  /**
   * SingleThreadRecordHandler Constructor
   *
   * @param dedupStrategy         The dedup strategy
   * @param dedupQueue            The dedup queue
   * @param sender                The record sender
   * @param clock                 The clock
   * @param dedupCompleteListener Optional dedup complete callback
   */
  public SingleThreadRecordHandler(DedupStrategy<K, V> dedupStrategy,
                                   DedupQueue<K, V> dedupQueue,
                                   RecordSender<K, V> sender,
                                   Clock clock,
                                   DedupCompleteListener<K, V> dedupCompleteListener) {
    this.dedupStrategy = dedupStrategy;
    this.dedupQueue = dedupQueue;
    this.sender = sender;
    this.clock = clock;
    this.dedupCompleteListener = dedupCompleteListener;
  }

  @Override
  public void start() {

  }

  @Override
  public void close() throws Exception {
    dedupQueue.close();
    sender.close();
  }

  @Override
  public void handle(final ConsumerRecord<K, V> record, final int topicIdx) {
    logger.debug("Handling {}", record);

    // update offset map
    offsets.offset(record.partition(), topicIdx, record.offset());

    DedupResult result = dedupStrategy.check(record);
    switch(result) {
      case DROP:
        logger.debug("Dropping {}", record);
        break;
      case SEND:
        logger.debug("Sending {}", record);
        send(record);
        break;
      case QUEUE:
        logger.debug("Enqueueing {}", record);
        enqueue(record, topicIdx);
        break;
    }

  }

  @Override
  public void tryDequeue(final int partition) {
    int totalDropped = 0;
    int totalSent = 0;
    if(!dedupQueue.isEmpty(partition)) {
      boolean sentSomething = false;
      do {
        for(int topicIdx = 0; topicIdx < numTopics; topicIdx++) {
          final ConsumerRecord<K, V> topRecord = dedupQueue.peek(partition, topicIdx);
          if(topRecord != null) {
            DedupResult result = dedupStrategy.check(topRecord);
            switch(result) {
              case DROP:
                final ConsumerRecord<K, V> droppedRecord = dedupQueue.poll(partition, topicIdx);
                logger.debug("Dropped from Queue: {}", droppedRecord);
                totalDropped++;
                break;
              case SEND:
                final ConsumerRecord<K, V> sendRecord = dedupQueue.poll(partition, topicIdx);
                logger.debug("Sending from Queue: {}", sendRecord);
                send(sendRecord);
                totalSent++;
                sentSomething = true;
                break;
              case QUEUE:
                // leave at top of queue
                break;
            }
          }
        }
      } while(sentSomething && !dedupQueue.isEmpty(partition));
    }
    if(totalSent > 0) {
      logger.info("Partition {} sent {} records from queue", partition, totalSent);
    }
    if(totalDropped > 0) {
      logger.info("Partition {} dropped {} duplicate records from queue", partition, totalDropped);
    }
  }

  private void send(final ConsumerRecord<K, V> record) {
    final RecordHeaders headers = headers(record.headers(), offsets);
    dedupStrategy.populateHeaders(record, headers);
    sender.send(record.partition(), record.key(), record.value(), headers);
    if(dedupCompleteListener != null) {
      dedupCompleteListener.onDedupComplete(record, headers);
    }
  }

  private RecordHeaders headers(Headers inboundHeaders, PartitionOffsets offsets) {
    final RecordHeaders headers = new RecordHeaders();
    for(Header inboundHeader : inboundHeaders) {
      headers.add(inboundHeader);
    }
    headers.add(RecordHeaderKeys.HEADER_KEY_DEDUP_SEND_TIME, BitUtil.longToBytes(clock.now()));
    offsets.populateHeaders(headers);
    return headers;
  }

  private void enqueue(final ConsumerRecord<K, V> record, final int topicIdx) {
    if(dedupQueue.isEmpty(record.partition(), topicIdx)) {
      logger.info("Starting to queue on topic={} partition={}", record.topic(), record.partition());
    }
    dedupQueue.add(record.partition(), topicIdx, record);
  }

  @Override
  public void assigned(final Assignment<K, V> assignment) {
    this.numTopics = assignment.getNumTopics();
    this.offsets = assignment.getOffsets();
    sender.open();
    dedupQueue.assigned(assignment);
    dedupStrategy.assigned(assignment);
  }

  @Override
  public void revoked() {
    sender.close();
    dedupQueue.revoked();
    dedupStrategy.revoked();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
