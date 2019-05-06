package io.thill.kafkacap.dedup.inbound;

import io.thill.kafkacap.dedup.handler.RecordHandler;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class LeadConsumer<K, V> implements Runnable, AutoCloseable {

  private static final Duration POLL_DURATION = Duration.ofSeconds(1);

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final CountDownLatch closeComplete = new CountDownLatch(1);
  private final AtomicBoolean subscribed = new AtomicBoolean();

  private final Properties consumerProperties;
  private final String topic;
  private final int topicIdx;
  private final RecordHandler<K, V> handler;
  private final List<FollowConsumer<K, V>> followConsumers;
  private final ThrottledDequeuer throttledDequeuer;

  public LeadConsumer(Properties consumerProperties,
                      String topic,
                      int topicIdx,
                      RecordHandler<K, V> handler,
                      List<FollowConsumer<K, V>> followConsumers,
                      ThrottledDequeuer throttledDequeuer) {
    this.consumerProperties = consumerProperties;
    this.topic = topic;
    this.topicIdx = topicIdx;
    this.handler = handler;
    this.followConsumers = followConsumers;
    this.throttledDequeuer = throttledDequeuer;
  }

  public void start() {
    new Thread(this, "LeadConsumer:" + topic).start();
  }

  @Override
  public void run() {
    final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties);
    try {
      logger.info("Starting {} for topic {}", getClass().getSimpleName(), topic);
      consumer.subscribe(Arrays.asList(topic), rebalanceListener);
      logger.info("Entering Poll Loop");
      while(keepRunning.get()) {
        final ConsumerRecords<K, V> records = consumer.poll(POLL_DURATION);
        if(!records.isEmpty()) {
          for(ConsumerRecord<K, V> record : records) {
            handler.handle(record, topicIdx);
          }
        }
      }
    } finally {
      logger.info("Closing...");
      consumer.close();
      closeComplete.countDown();
      logger.info("Close Complete");
    }
  }

  @Override
  public void close() throws InterruptedException {
    keepRunning.set(false);
    closeComplete.await();
  }

  private final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
      final List<Integer> partitions = partitions(topicPartitions);
      logger.info("Partitions Revoked: {}", partitions);

      try {
        logger.info("Revoking partitions {} from {}", partitions, throttledDequeuer);
        throttledDequeuer.revoke(partitions);
      } catch(InterruptedException e) {
        logger.error("Exception while assigning to " + throttledDequeuer, e);
      }

      for(FollowConsumer fc : followConsumers) {
        try {
          logger.info("Revoking partitions {} from {}", partitions, fc);
          fc.revoke(Collections.emptyList());
        } catch(InterruptedException e) {
          logger.error("Exception while assigning to " + fc, e);
        }
      }

      logger.info("Revoking partitions {} from {}", partitions, handler.getClass().getSimpleName());
      handler.revoked(partitions, followConsumers.size() + 1);

      subscribed.set(false);
      logger.info("Revoke Complete");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
      final List<Integer> partitions = partitions(topicPartitions);
      logger.info("Partitions Assigned: {}", partitions);

      logger.info("Assigning partitions {} to {}", partitions, handler.getClass().getSimpleName());
      handler.assigned(partitions, followConsumers.size() + 1);

      try {
        logger.info("Assigning partitions {} to {}", partitions, throttledDequeuer);
        throttledDequeuer.assign(partitions);
      } catch(InterruptedException e) {
        logger.error("Exception while assigning to " + throttledDequeuer, e);
      }

      for(FollowConsumer fc : followConsumers) {
        try {
          logger.info("Assigning partitions {} to {}", partitions, fc);
          fc.assign(partitions);
        } catch(InterruptedException e) {
          logger.error("Exception while assigning to " + fc, e);
        }
      }

      subscribed.set(true);
      logger.info("Assignment Complete");
    }

    private List<Integer> partitions(Collection<TopicPartition> topicPartitions) {
      final List<Integer> partitions = new ArrayList<>();
      for(TopicPartition tp : topicPartitions)
        partitions.add(tp.partition());
      Collections.sort(partitions);
      return partitions;
    }
  };

  public boolean isSubscribed() {
    return subscribed.get();
  }

  @Override
  public String toString() {
    return "LeadConsumer{" +
            "topic='" + topic + '\'' +
            '}';
  }
}
