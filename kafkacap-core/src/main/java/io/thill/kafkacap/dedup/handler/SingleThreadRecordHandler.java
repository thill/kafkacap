/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.handler;

import io.thill.kafkacap.dedup.assignment.Assignment;
import io.thill.kafkacap.dedup.cache.RecordCacheManager;
import io.thill.kafkacap.dedup.callback.DedupCompleteListener;
import io.thill.kafkacap.dedup.outbound.RecordSender;
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
 * An implementation of a {@link RecordHandler} that handles flow between the {@link DedupStrategy}, {@link RecordCacheManager}, and {@link RecordSender}, but
 * is not thread-safe. It is meant to be encapsulated by a thread-safe implementation.
 *
 * @param <K> The kafka record key type
 * @param <V> The kafka record value type
 * @author Eric Thill
 */
public class SingleThreadRecordHandler<K, V> implements RecordHandler<K, V> {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final DedupStrategy<K, V> dedupStrategy;
  private final RecordCacheManager<K, V> recordCacheManager;
  private final boolean orderedCapture;
  private final RecordSender<K, V> sender;
  private final Clock clock;
  private final DedupCompleteListener<K, V> dedupCompleteListener;
  private int numTopics;
  private PartitionOffsets offsets;

  /**
   * SingleThreadRecordHandler Constructor
   *
   * @param dedupStrategy         The dedup strategy
   * @param recordCacheManager    The RecordCache manager
   * @param orderedCapture        Flag if the captured messages are ordered (true), or unordered (false)
   * @param sender                The record sender
   * @param clock                 The clock
   * @param dedupCompleteListener Optional dedup complete callback
   */
  public SingleThreadRecordHandler(DedupStrategy<K, V> dedupStrategy,
                                   RecordCacheManager<K, V> recordCacheManager,
                                   boolean orderedCapture,
                                   RecordSender<K, V> sender,
                                   Clock clock,
                                   DedupCompleteListener<K, V> dedupCompleteListener) {
    this.dedupStrategy = dedupStrategy;
    this.recordCacheManager = recordCacheManager;
    this.orderedCapture = orderedCapture;
    this.sender = sender;
    this.clock = clock;
    this.dedupCompleteListener = dedupCompleteListener;
  }

  @Override
  public void start() {

  }

  @Override
  public void close() {
    recordCacheManager.close();
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
      case CACHE:
        logger.debug("Enqueueing {}", record);
        addToCache(record, topicIdx);
        break;
    }
  }

  @Override
  public void flush() {
    sender.flush();
  }

  @Override
  public void checkCache(final int partition) {
    if(orderedCapture) {
      checkCacheOrdered(partition);
    } else {
      checkCacheUnordered(partition);
    }
  }

  private void checkCacheOrdered(final int partition) {
    int totalDropped = 0;
    int totalSent = 0;
    if(!recordCacheManager.isEmpty(partition)) {
      boolean sentSomething = false;
      do {
        for(int topicIdx = 0; topicIdx < numTopics; topicIdx++) {
          final ConsumerRecord<K, V> topRecord = recordCacheManager.peek(partition, topicIdx);
          if(topRecord != null) {
            DedupResult result = dedupStrategy.check(topRecord);
            switch(result) {
              case DROP:
                final ConsumerRecord<K, V> droppedRecord = recordCacheManager.poll(partition, topicIdx);
                logger.debug("Dropped from Cache: {}", droppedRecord);
                totalDropped++;
                break;
              case SEND:
                final ConsumerRecord<K, V> sendRecord = recordCacheManager.poll(partition, topicIdx);
                logger.debug("Sending from Cache: {}", sendRecord);
                send(sendRecord);
                totalSent++;
                sentSomething = true;
                break;
              case CACHE:
                // leave at top of queue
                break;
            }
          }
        }
      } while(sentSomething && !recordCacheManager.isEmpty(partition));
    }
    if(totalSent > 0) {
      logger.info("Partition {} sent {} records from cache", partition, totalSent);
    }
    if(totalDropped > 0) {
      logger.info("Partition {} dropped {} duplicate records from cache", partition, totalDropped);
    }
  }

  private void checkCacheUnordered(final int partition) {
    int totalDropped = 0;
    int totalSent = 0;
    if(!recordCacheManager.isEmpty(partition)) {
      boolean sentSomething = false;
      do {
        for(int topicIdx = 0; topicIdx < numTopics; topicIdx++) {
          for(int recordIdx = 0; recordIdx < recordCacheManager.size(partition, topicIdx); recordIdx++) {
            final ConsumerRecord<K, V> record = recordCacheManager.get(partition, topicIdx, recordIdx);
            DedupResult result = dedupStrategy.check(record);
            switch(result) {
              case DROP:
                final ConsumerRecord<K, V> droppedRecord = recordCacheManager.remove(partition, topicIdx, recordIdx);
                logger.debug("Dropped from Cache: {}", droppedRecord);
                totalDropped++;
                break;
              case SEND:
                final ConsumerRecord<K, V> sendRecord = recordCacheManager.remove(partition, topicIdx, recordIdx);
                logger.debug("Sending from Cache: {}", sendRecord);
                send(sendRecord);
                totalSent++;
                sentSomething = true;
                break;
              case CACHE:
                // leave in cache
                break;
            }
          }
        }
      } while(sentSomething && !recordCacheManager.isEmpty(partition));
    }
    if(totalSent > 0) {
      logger.info("Partition {} sent {} records from cache", partition, totalSent);
    }
    if(totalDropped > 0) {
      logger.info("Partition {} dropped {} duplicate records from cache", partition, totalDropped);
    }
  }

  private void send(final ConsumerRecord<K, V> record) {
    final RecordHeaders headers = headers(record.headers(), offsets, record.partition());
    dedupStrategy.populateHeaders(record, headers);
    sender.send(record.partition(), record.key(), record.value(), headers);
    if(dedupCompleteListener != null) {
      dedupCompleteListener.onDedupComplete(record, headers);
    }
  }

  private RecordHeaders headers(Headers inboundHeaders, PartitionOffsets offsets, int partition) {
    final RecordHeaders headers = new RecordHeaders();
    for(Header inboundHeader : inboundHeaders) {
      headers.add(inboundHeader);
    }
    headers.add(RecordHeaderKeys.HEADER_KEY_DEDUP_SEND_TIME, BitUtil.longToBytes(clock.now()));
    offsets.populateHeaders(headers, partition);
    return headers;
  }

  private void addToCache(final ConsumerRecord<K, V> record, final int topicIdx) {
    if(recordCacheManager.isEmpty(record.partition(), topicIdx)) {
      logger.info("Starting to queue on topic={} partition={}", record.topic(), record.partition());
    }
    recordCacheManager.add(record.partition(), topicIdx, record);
  }

  @Override
  public void assigned(final Assignment<K, V> assignment) {
    this.numTopics = assignment.getNumTopics();
    this.offsets = assignment.getOffsets();
    sender.open();
    recordCacheManager.assigned(assignment);
    dedupStrategy.assigned(assignment);
  }

  @Override
  public void revoked() {
    sender.close();
    recordCacheManager.revoked();
    dedupStrategy.revoked();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
