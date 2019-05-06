package io.thill.kafkacap.dedup.inbound;

import io.thill.kafkacap.dedup.handler.RecordHandler;
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
import java.util.concurrent.atomic.AtomicReference;

public class FollowConsumer<K, V> implements Runnable, AutoCloseable {

  private static final Duration POLL_DURATION = Duration.ofSeconds(1);
  private static final Duration POLL_SLEEP_DURACTION = Duration.ofMillis(250);
  private static final Duration ASSIGN_SLEEP_DURACTION = Duration.ofMillis(10);

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final CountDownLatch closeComplete = new CountDownLatch(1);
  private final AtomicReference<Collection<Integer>> nextAssignment = new AtomicReference<>();

  private final Properties consumerProperties;
  private final String topic;
  private final int topicIdx;
  private final RecordHandler<K, V> handler;

  public FollowConsumer(Properties consumerProperties, String topic, int topicIdx, RecordHandler<K, V> handler) {
    this.consumerProperties = consumerProperties;
    this.topic = topic;
    this.topicIdx = topicIdx;
    this.handler = handler;
  }

  public void start() {
    new Thread(this, "FollowConsumer:" + topic).start();
  }

  @Override
  public void run() {
    final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties);
    Collection<Integer> currentAssignment = Collections.emptyList();
    try {
      logger.info("Starting {} for topic {}", getClass().getSimpleName(), topic);
      logger.info("Entering Poll Loop");
      while(keepRunning.get()) {
        ConsumerRecords<K, V> records;
        if(currentAssignment.size() == 0) {
          // nothing assigned, sleep a bit
          Thread.sleep(POLL_SLEEP_DURACTION.toMillis());
          records = ConsumerRecords.empty();
        } else {
          // poll records
          records = consumer.poll(POLL_DURATION);
        }

        // check for new assignment
        Collection<Integer> newAssignment = nextAssignment.getAndSet(null);
        if(newAssignment != null) {
          currentAssignment = newAssignment;
          consumer.assign(topicPartitions(newAssignment));
          logger.info("New Assignment: {}", newAssignment);
          records = ConsumerRecords.empty(); // new assignment: drop records from prior poll
        }

        if(!records.isEmpty()) {
          for(ConsumerRecord<K, V> record : records) {
            handler.handle(record, topicIdx);
          }
        }
      }
    } catch(Throwable t) {
      logger.error("Unhandled Exception", t);
    } finally {
      logger.info("Closing...");
      consumer.close();
      closeComplete.countDown();
      logger.info("Close Complete");
    }
  }

  private Collection<TopicPartition> topicPartitions(Collection<Integer> partitions) {
    List<TopicPartition> topicPartitions = new ArrayList<>();
    for(Integer partition : partitions)
      topicPartitions.add(new TopicPartition(topic, partition));
    return topicPartitions;
  }

  @Override
  public void close() throws InterruptedException {
    keepRunning.set(false);
    closeComplete.await();
  }

  public void assign(List<Integer> partitions) throws InterruptedException {
    nextAssignment.set(partitions);
    // block until assignment is accepted
    while(nextAssignment.get() != null) {
      Thread.sleep(ASSIGN_SLEEP_DURACTION.toMillis());
    }
  }

  public void revoke(List<Integer> partitions) throws InterruptedException {
    nextAssignment.set(Collections.emptyList());
    // block until assignment is accepted
    while(nextAssignment.get() != null) {
      Thread.sleep(ASSIGN_SLEEP_DURACTION.toMillis());
    }
  }

  @Override
  public String toString() {
    return "FollowConsumer{" +
            "topic='" + topic + '\'' +
            '}';
  }
}
