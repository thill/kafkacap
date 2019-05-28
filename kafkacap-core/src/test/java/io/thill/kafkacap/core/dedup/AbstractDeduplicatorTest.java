/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup;

import io.thill.kafkacap.core.dedup.strategy.TestableSequencedDedupStrategy;
import io.thill.kafkalite.KafkaLite;
import io.thill.kafkalite.client.QueuedKafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Properties;

public abstract class AbstractDeduplicatorTest {

  protected static final String CAPTURE_TOPIC_A = "cap_a";
  protected static final String CAPTURE_TOPIC_B = "cap_b";
  protected static final String CAPTURE_TOPIC_C = "cap_c";
  protected static final String DEDUP_TOPIC = "dedup";

  protected static final int PARTITION_0 = 0;
  protected static final int PARTITION_1 = 1;

  protected static final int NUM_PARTITIONS = 2;
  protected static final int GAP_TIMEOUT = 5000;
  protected static final int WAIT_FOR_GAP_TIME = GAP_TIMEOUT * 2;

  protected Deduplicator<Long, String> deduplicator;
  protected QueuedKafkaConsumer<String, String> consumer0;
  protected QueuedKafkaConsumer<String, String> consumer1;
  protected KafkaProducer<Long, String> producer;

  @Before
  public void setup() throws Exception {
    KafkaLite.reset();
    KafkaLite.cleanOnShutdown();
    KafkaLite.createTopic(CAPTURE_TOPIC_A, NUM_PARTITIONS);
    KafkaLite.createTopic(CAPTURE_TOPIC_B, NUM_PARTITIONS);
    KafkaLite.createTopic(CAPTURE_TOPIC_C, NUM_PARTITIONS);
    KafkaLite.createTopic(DEDUP_TOPIC, NUM_PARTITIONS);

    consumer0 = new QueuedKafkaConsumer<>(new TopicPartition(DEDUP_TOPIC, PARTITION_0), KafkaLite.consumerProperties(LongDeserializer.class, StringDeserializer.class));
    consumer1 = new QueuedKafkaConsumer<>(new TopicPartition(DEDUP_TOPIC, PARTITION_1), KafkaLite.consumerProperties(LongDeserializer.class, StringDeserializer.class));
    producer = new KafkaProducer<>(KafkaLite.producerProperties(LongSerializer.class, StringSerializer.class));

    startDeduplicator();
  }

  protected void startDeduplicator() throws InterruptedException {
    Properties consumerProperties = KafkaLite.consumerProperties(LongDeserializer.class, StringDeserializer.class);
    Properties producerProperties = KafkaLite.producerProperties(LongSerializer.class, StringSerializer.class);
    deduplicator = new DeduplicatorBuilder<Long, String>()
            .consumerGroupIdPrefix("test_group_")
            .consumerProperties(consumerProperties)
            .producerProperties(producerProperties)
            .dedupStrategy(new TestableSequencedDedupStrategy(isOrderedCapture(), GAP_TIMEOUT))
            .inboundTopics(Arrays.asList(CAPTURE_TOPIC_A, CAPTURE_TOPIC_B, CAPTURE_TOPIC_C))
            .outboundTopic(DEDUP_TOPIC)
            .orderedCapture(isOrderedCapture())
            .build();

    deduplicator.start();
    while(!deduplicator.isSubscribed())
      Thread.sleep(50);
  }

  @After
  public void shutdown() {
    if(deduplicator != null) {
      deduplicator.close();
      deduplicator = null;
    }
    if(consumer0 != null) {
      consumer0.close();
      consumer0 = null;
    }
    if(consumer1 != null) {
      consumer1.close();
      consumer1 = null;
    }
  }

  protected abstract boolean isOrderedCapture();

  protected void send(String topic, int partition, long sequence) {
    producer.send(new ProducerRecord<>(topic, partition, null, sequence, Long.toString(sequence)));
  }

  protected void sendAllTopics(int partition, long sequence) {
    send(CAPTURE_TOPIC_A, partition, sequence);
    send(CAPTURE_TOPIC_B, partition, sequence);
    send(CAPTURE_TOPIC_C, partition, sequence);
  }

}
