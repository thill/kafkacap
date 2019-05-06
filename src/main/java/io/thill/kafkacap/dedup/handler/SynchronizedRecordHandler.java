package io.thill.kafkacap.dedup.handler;

import io.thill.kafkacap.dedup.outbound.RecordSender;
import io.thill.kafkacap.dedup.queue.DedupQueue;
import io.thill.kafkacap.dedup.strategy.DedupResult;
import io.thill.kafkacap.dedup.strategy.DedupStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class SynchronizedRecordHandler<K, V> implements RecordHandler<K, V> {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final DedupStrategy<K, V> dedupStrategy;
  private final DedupQueue<K, V> dedupQueue;
  private final RecordSender<K, V> sender;
  private int numTopics;

  public SynchronizedRecordHandler(DedupStrategy<K, V> dedupStrategy, DedupQueue<K, V> dedupQueue, RecordSender<K, V> sender) {
    this.dedupStrategy = dedupStrategy;
    this.dedupQueue = dedupQueue;
    this.sender = sender;
  }

  @Override
  public synchronized void handle(final ConsumerRecord<K, V> record, final int topicIdx) {
    logger.debug("Handling {}", record);

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
    sender.send(record.partition(), record.key(), record.value(), record.headers());
  }

  private void enqueue(final ConsumerRecord<K, V> record, final int topicIdx) {
    if(dedupQueue.isEmpty(record.partition(), topicIdx)) {
      logger.info("Starting to queue on topic={} partition={}", record.topic(), record.partition());
    }
    dedupQueue.add(record.partition(), topicIdx, record);
  }

  @Override
  public synchronized void assigned(final Collection<Integer> partitions, final int numTopics) {
    this.numTopics = numTopics;
    dedupQueue.assigned(partitions, numTopics);
    dedupStrategy.assigned(partitions, numTopics);
  }

  @Override
  public synchronized void revoked(final Collection<Integer> partitions, final int numTopics) {
    dedupQueue.revoked(partitions, numTopics);
    dedupStrategy.revoked(partitions, numTopics);
  }

}
