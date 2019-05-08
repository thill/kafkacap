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
  private final AtomicReference<Assignment> nextAssignment = new AtomicReference<>();

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
        Assignment newAssignment = nextAssignment.getAndSet(null);
        if(newAssignment != null) {
          logger.info("New Assignment: {}", newAssignment);

          // new assignment: drop records from prior poll
          records = ConsumerRecords.empty();

          // assign new partitions
          currentAssignment = newAssignment.partitions;
          consumer.assign(topicPartitions(newAssignment.partitions));

          // seek to recovered offset+1 for each partition
          for(final int partition : currentAssignment) {
            final Long recoveredOffset = newAssignment.partitionOffsets.get(partition);
            if(recoveredOffset == null) {
              logger.info("Seeking {} to beginning", topic, partition);
              consumer.seekToBeginning(Arrays.asList(new TopicPartition(topic, partition)));
            } else {
              logger.info("Seeking {} to {}", topic, partition);
              consumer.seek(new TopicPartition(topic, partition), recoveredOffset + 1);
            }
          }
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

  public void assign(List<Integer> partitions, Map<Integer, Long> partitionOffsets) throws InterruptedException {
    nextAssignment.set(new Assignment(partitions, partitionOffsets));
    // block until assignment is accepted
    while(nextAssignment.get() != null) {
      Thread.sleep(ASSIGN_SLEEP_DURACTION.toMillis());
    }
  }

  public void revoke() throws InterruptedException {
    nextAssignment.set(new Assignment(Collections.emptyList(), Collections.emptyMap()));
    // block until assignment is accepted
    while(nextAssignment.get() != null) {
      Thread.sleep(ASSIGN_SLEEP_DURACTION.toMillis());
    }
  }

  public int getTopicIdx() {
    return topicIdx;
  }

  @Override
  public String toString() {
    return "FollowConsumer{" +
            "topic='" + topic + '\'' +
            '}';
  }

  private static class Assignment {
    private final Collection<Integer> partitions;
    private final Map<Integer, Long> partitionOffsets;

    public Assignment(Collection<Integer> partitions, Map<Integer, Long> partitionOffsets) {
      this.partitions = partitions;
      this.partitionOffsets = partitionOffsets;
    }

    @Override
    public String toString() {
      return "Assignment{" +
              "partitions=" + partitions +
              ", partitionOffsets=" + partitionOffsets +
              '}';
    }
  }
}
