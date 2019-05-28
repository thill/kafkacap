/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.inbound;

import io.thill.kafkacap.dedup.handler.RecordHandler;
import io.thill.kafkacap.dedup.assignment.Assignment;
import io.thill.kafkacap.dedup.recovery.RecoveryService;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A kafka consumer that subscribes to a single inbound kafka topic using a consumer group for partition assignment. Partition assignment/revocation callbacks
 * are dispatched to underling {@link FollowConsumer}s and a {@link ThrottledDequeuer}.
 *
 * @param <K> The {@link ConsumerRecord} key type
 * @param <V> The {@link ConsumerRecord} value type
 * @author Eric Thill
 */
public class LeadConsumer<K, V> implements Runnable, AutoCloseable {

  private static final Duration POLL_DURATION = Duration.ofSeconds(1);
  private static final String DEFAULT_OFFSET_COMMIT_INTERVAL = "10000";

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
  private final RecoveryService<K, V> recoveryService;
  private final long offsetCommitInterval;

  /**
   * LeadConsumer Constructor
   *
   * @param consumerProperties The properties used to instantiate the underling {@link KafkaConsumer}
   * @param topic              The inbound kafka topic
   * @param topicIdx           The index assigned to the inbound kafka topic
   * @param handler            The handler used to dispatch all received records
   * @param followConsumers    The follow consumers that require partition assignment callbacks
   * @param throttledDequeuer  The throttled dequerer that requires partition assignment callbacks
   * @param recoveryService    The recovery service used to poll last published records for all assignment partitions
   */
  public LeadConsumer(Properties consumerProperties,
                      String topic,
                      int topicIdx,
                      RecordHandler<K, V> handler,
                      List<FollowConsumer<K, V>> followConsumers,
                      ThrottledDequeuer throttledDequeuer,
                      RecoveryService<K, V> recoveryService) {
    this.topic = topic;
    this.topicIdx = topicIdx;
    this.handler = handler;
    this.followConsumers = followConsumers;
    this.throttledDequeuer = throttledDequeuer;
    this.recoveryService = recoveryService;
    this.consumerProperties = new Properties();
    this.consumerProperties.putAll(consumerProperties);
    this.consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
    this.offsetCommitInterval = Long.parseLong(consumerProperties.getProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, DEFAULT_OFFSET_COMMIT_INTERVAL));
  }

  /**
   * Start the run loop in a new thread
   */
  public void start() {
    new Thread(this, "LeadConsumer:" + topic).start();
  }

  @Override
  public void run() {
    final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties);
    long nextCommitTime = System.currentTimeMillis() + offsetCommitInterval;

    try {
      logger.info("Starting {} for topic {}", getClass().getSimpleName(), topic);
      consumer.subscribe(Arrays.asList(topic), rebalanceListener);
      logger.info("Entering Poll Loop");
      while(keepRunning.get()) {
        // poll and handle records
        final ConsumerRecords<K, V> records = consumer.poll(POLL_DURATION);
        if(!records.isEmpty()) {
          for(ConsumerRecord<K, V> record : records) {
            handler.handle(record, topicIdx);
          }
        }

        // check commit
        final long now = System.currentTimeMillis();
        if(now >= nextCommitTime) {
          logger.debug("Performing Commit");
          handler.flush();
          consumer.commitAsync();
          logger.debug("Commit Complete");
          nextCommitTime = now + offsetCommitInterval;
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
          fc.revoke();
        } catch(InterruptedException e) {
          logger.error("Exception while assigning to " + fc, e);
        }
      }

      logger.info("Revoking partitions {} from {}", partitions, handler.getClass().getSimpleName());
      handler.revoked();

      subscribed.set(false);
      logger.info("Revoke Complete");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
      final List<Integer> partitions = partitions(topicPartitions);
      logger.info("Partitions Assigned: {}", partitions);

      logger.info("Recovering Inbound Offsets from Last Outbound Record Headers");
      final Assignment<K, V> assignment = recoveryService.recover(partitions);

      logger.info("Assigning partitions {} to {}", partitions, handler.getClass().getSimpleName());
      handler.assigned(assignment);

      try {
        logger.info("Assigning partitions {} to {}", partitions, throttledDequeuer);
        throttledDequeuer.assign(partitions);
      } catch(InterruptedException e) {
        logger.error("Exception while assigning to " + throttledDequeuer, e);
      }

      for(FollowConsumer fc : followConsumers) {
        try {
          logger.info("Assigning partitions {} to {}", partitions, fc);
          fc.assign(partitions, assignment.getOffsets().topicOffsets(fc.getTopicIdx()));
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
