package io.thill.kafkacap.dedup;

import io.thill.kafkacap.dedup.strategy.TestableSequencedDedupStrategy;
import io.thill.kafkalite.KafkaLite;
import io.thill.kafkalite.client.QueuedKafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

public class TestDedeuplicator {

  private static final String CAPTURE_TOPIC_A = "cap_a";
  private static final String CAPTURE_TOPIC_B = "cap_b";
  private static final String CAPTURE_TOPIC_C = "cap_c";
  private static final String DEDUP_TOPIC = "dedup";

  private static final int PARTITION_0 = 0;
  private static final int PARTITION_1 = 1;

  private static final int NUM_PARTITIONS = 2;
  private static final int GAP_TIMEOUT = 5000;
  private static final int WAIT_FOR_GAP_TIME = GAP_TIMEOUT * 2;

  private Deduplicator<String, String> deduplicator;
  private QueuedKafkaConsumer<String, String> consumer0;
  private QueuedKafkaConsumer<String, String> consumer1;
  private KafkaProducer<String, String> producer;

  @Before
  public void setup() throws Exception {
    KafkaLite.reset();
    KafkaLite.cleanOnShutdown();
    KafkaLite.createTopic(CAPTURE_TOPIC_A, NUM_PARTITIONS);
    KafkaLite.createTopic(CAPTURE_TOPIC_B, NUM_PARTITIONS);
    KafkaLite.createTopic(CAPTURE_TOPIC_C, NUM_PARTITIONS);
    KafkaLite.createTopic(DEDUP_TOPIC, NUM_PARTITIONS);

    consumer0 = new QueuedKafkaConsumer<>(new TopicPartition(DEDUP_TOPIC, PARTITION_0), KafkaLite.consumerProperties(StringDeserializer.class, StringDeserializer.class));
    consumer1 = new QueuedKafkaConsumer<>(new TopicPartition(DEDUP_TOPIC, PARTITION_1), KafkaLite.consumerProperties(StringDeserializer.class, StringDeserializer.class));
    producer = new KafkaProducer<>(KafkaLite.producerProperties(StringSerializer.class, StringSerializer.class));

    Properties consumerProperties = KafkaLite.consumerProperties(StringDeserializer.class, StringDeserializer.class);
    Properties producerProperties = KafkaLite.producerProperties(StringSerializer.class, StringSerializer.class);

    deduplicator = new DeduplicatorBuilder<String, String>()
            .consumerGroupIdPrefix("test_group_")
            .consumerProperties(consumerProperties)
            .producerProperties(producerProperties)
            .dedupStrategy(new TestableSequencedDedupStrategy(GAP_TIMEOUT))
            .inboundTopics(Arrays.asList(CAPTURE_TOPIC_A, CAPTURE_TOPIC_B, CAPTURE_TOPIC_C))
            .outboundTopic(DEDUP_TOPIC)
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

  private void send(String topic, int partition, long sequence) {
    producer.send(new ProducerRecord<>(topic, partition, null, Long.toString(sequence), Long.toString(sequence)));
  }

  private void sendAllTopics(int partition, long sequence) {
    send(CAPTURE_TOPIC_A, partition, sequence);
    send(CAPTURE_TOPIC_B, partition, sequence);
    send(CAPTURE_TOPIC_C, partition, sequence);
  }

  @Test
  public void test_no_gaps() throws Exception {
    sendAllTopics(PARTITION_0, 10000);
    sendAllTopics(PARTITION_0, 10001);
    sendAllTopics(PARTITION_0, 10002);

    sendAllTopics(PARTITION_1, 100);
    sendAllTopics(PARTITION_1, 101);
    sendAllTopics(PARTITION_1, 102);

    Assert.assertEquals("10000", consumer0.poll().value());
    Assert.assertEquals("10001", consumer0.poll().value());
    Assert.assertEquals("10002", consumer0.poll().value());

    Assert.assertEquals("100", consumer1.poll().value());
    Assert.assertEquals("101", consumer1.poll().value());
    Assert.assertEquals("102", consumer1.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
    Assert.assertTrue(consumer1.isEmpty());
  }

  @Test
  public void test_gap_on_lead_topic() throws Exception {
    send(CAPTURE_TOPIC_A, PARTITION_0, 100);
    send(CAPTURE_TOPIC_A, PARTITION_0, 101);
    send(CAPTURE_TOPIC_A, PARTITION_0, 105);

    send(CAPTURE_TOPIC_B, PARTITION_0, 100);
    send(CAPTURE_TOPIC_B, PARTITION_0, 101);
    send(CAPTURE_TOPIC_B, PARTITION_0, 102);
    send(CAPTURE_TOPIC_B, PARTITION_0, 103);
    send(CAPTURE_TOPIC_B, PARTITION_0, 104);
    send(CAPTURE_TOPIC_B, PARTITION_0, 105);

    Assert.assertEquals("100", consumer0.poll().value());
    Assert.assertEquals("101", consumer0.poll().value());
    Assert.assertEquals("102", consumer0.poll().value());
    Assert.assertEquals("103", consumer0.poll().value());
    Assert.assertEquals("104", consumer0.poll().value());
    Assert.assertEquals("105", consumer0.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
  }

  @Test
  public void test_gap_on_follow_topic() throws Exception {
    send(CAPTURE_TOPIC_A, PARTITION_0, 100);
    send(CAPTURE_TOPIC_A, PARTITION_0, 101);
    send(CAPTURE_TOPIC_A, PARTITION_0, 102);
    send(CAPTURE_TOPIC_A, PARTITION_0, 103);
    send(CAPTURE_TOPIC_A, PARTITION_0, 104);
    send(CAPTURE_TOPIC_A, PARTITION_0, 105);

    send(CAPTURE_TOPIC_B, PARTITION_0, 100);
    send(CAPTURE_TOPIC_B, PARTITION_0, 101);
    send(CAPTURE_TOPIC_B, PARTITION_0, 105);

    Assert.assertEquals("100", consumer0.poll().value());
    Assert.assertEquals("101", consumer0.poll().value());
    Assert.assertEquals("102", consumer0.poll().value());
    Assert.assertEquals("103", consumer0.poll().value());
    Assert.assertEquals("104", consumer0.poll().value());
    Assert.assertEquals("105", consumer0.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
  }

  @Test
  public void test_gap_on_all_topics() throws Exception {
    send(CAPTURE_TOPIC_A, PARTITION_0, 100);
    send(CAPTURE_TOPIC_A, PARTITION_0, 101);
    send(CAPTURE_TOPIC_A, PARTITION_0, 105);

    send(CAPTURE_TOPIC_B, PARTITION_0, 100);
    send(CAPTURE_TOPIC_B, PARTITION_0, 101);
    send(CAPTURE_TOPIC_B, PARTITION_0, 105);

    send(CAPTURE_TOPIC_C, PARTITION_0, 100);
    send(CAPTURE_TOPIC_C, PARTITION_0, 101);
    send(CAPTURE_TOPIC_C, PARTITION_0, 105);

    Assert.assertEquals("100", consumer0.poll().value());

    // available immediately when gap is on all streams
    Assert.assertEquals("101", consumer0.poll().value());
    Assert.assertEquals("105", consumer0.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
  }

  @Test
  public void test_gap_on_two_active_topics() throws Exception {
    send(CAPTURE_TOPIC_A, PARTITION_0, 100);
    send(CAPTURE_TOPIC_A, PARTITION_0, 101);
    send(CAPTURE_TOPIC_A, PARTITION_0, 105);

    send(CAPTURE_TOPIC_B, PARTITION_0, 100);
    send(CAPTURE_TOPIC_B, PARTITION_0, 101);
    send(CAPTURE_TOPIC_B, PARTITION_0, 105);

    // first message2 available immediately
    Assert.assertEquals("100", consumer0.poll().value());
    Assert.assertEquals("101", consumer0.poll().value());

    // message after not available yet (must wait for timeout)
    Assert.assertTrue(consumer0.isEmpty());

    Thread.sleep(WAIT_FOR_GAP_TIME);

    // message available after timeout
    Assert.assertEquals("105", consumer0.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
  }



}
