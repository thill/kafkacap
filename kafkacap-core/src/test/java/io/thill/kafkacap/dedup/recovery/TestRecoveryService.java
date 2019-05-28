/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.recovery;

import io.thill.kafkacap.dedup.assignment.Assignment;
import io.thill.kafkacap.util.constant.RecordHeaderKeys;
import io.thill.kafkacap.util.io.BitUtil;
import io.thill.kafkalite.KafkaLite;
import io.thill.kafkalite.client.QueuedKafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class TestRecoveryService {

  private static final String TOPIC = "TestRecoveryService";
  private static final int PARTITION_0 = 0;
  private static final int PARTITION_1 = 1;
  private static final int NUM_PARTITIONS = 2;
  private static final int NUM_INBOUND_TOPICS = 3;

  private RecoveryService recoveryService;
  private KafkaProducer<String, String> producer;
  private QueuedKafkaConsumer<String, String> consumer0;
  private QueuedKafkaConsumer<String, String> consumer1;

  @Before
  public void setup() throws Exception {
    KafkaLite.reset();
    KafkaLite.cleanOnShutdown();
    KafkaLite.createTopic(TOPIC, NUM_PARTITIONS);
    recoveryService = new LastRecordRecoveryService(KafkaLite.consumerProperties(StringDeserializer.class, StringDeserializer.class), TOPIC, NUM_INBOUND_TOPICS);
    producer = new KafkaProducer<>(KafkaLite.producerProperties(StringSerializer.class, StringSerializer.class));
    consumer0 = new QueuedKafkaConsumer<>(new TopicPartition(TOPIC, PARTITION_0), KafkaLite.consumerProperties(StringDeserializer.class, StringDeserializer.class));
    consumer1 = new QueuedKafkaConsumer<>(new TopicPartition(TOPIC, PARTITION_1), KafkaLite.consumerProperties(StringDeserializer.class, StringDeserializer.class));
  }

  @After
  public void cleanup() {
    if(producer != null) {
      producer.close();
      producer = null;
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

  @Test
  public void test_one_partition_no_record() {
    Assignment<String, String> assignment = recoveryService.recover(Arrays.asList(PARTITION_0));
    Assert.assertEquals(0, assignment.getOffsets().size());
  }

  @Test
  public void test_one_partition_one_record_no_headers() {
    send(0);

    Assert.assertNotNull(consumer0.poll()); // waits for async publish

    Assignment<String, String> assignment = recoveryService.recover(Arrays.asList(PARTITION_0));
    Assert.assertEquals(0, assignment.getOffsets().size());
  }

  @Test
  public void test_one_partition_one_record_one_header() {
    send(0,
            new TopicOffset(0, 100)
    );

    Assert.assertNotNull(consumer0.poll()); // waits for async publish

    Assignment<String, String> assignment = recoveryService.recover(Arrays.asList(PARTITION_0));
    Assert.assertEquals(1, assignment.getOffsets().size());
    Assert.assertEquals(100, (long)assignment.getOffsets().get(new PartitionTopicIdx(0, 0)));
  }

  @Test
  public void test_one_partition_one_record_multiple_headers() {
    send(0,
            new TopicOffset(0, 100),
            new TopicOffset(1, 200),
            new TopicOffset(2, 311)
    );

    Assert.assertNotNull(consumer0.poll()); // waits for async publish

    Assignment<String, String> assignment = recoveryService.recover(Arrays.asList(PARTITION_0));
    Assert.assertEquals(3, assignment.getOffsets().size());
    Assert.assertEquals(100, (long)assignment.getOffsets().get(new PartitionTopicIdx(0, 0)));
    Assert.assertEquals(200, (long)assignment.getOffsets().get(new PartitionTopicIdx(0, 1)));
    Assert.assertEquals(311, (long)assignment.getOffsets().get(new PartitionTopicIdx(0, 2)));
  }

  @Test
  public void test_one_partition_multiple_record() {
    send(0,
            new TopicOffset(0, 100),
            new TopicOffset(1, 200),
            new TopicOffset(2, 311)
    );

    Assert.assertNotNull(consumer0.poll()); // waits for async publish

    send(0,
            new TopicOffset(0, 101),
            new TopicOffset(1, 201),
            new TopicOffset(2, 312)
    );

    Assert.assertNotNull(consumer0.poll()); // waits for async publish

    Assignment<String, String> assignment = recoveryService.recover(Arrays.asList(PARTITION_0));
    Assert.assertEquals(3, assignment.getOffsets().size());
    Assert.assertEquals(101, (long)assignment.getOffsets().get(new PartitionTopicIdx(0, 0)));
    Assert.assertEquals(201, (long)assignment.getOffsets().get(new PartitionTopicIdx(0, 1)));
    Assert.assertEquals(312, (long)assignment.getOffsets().get(new PartitionTopicIdx(0, 2)));
  }

  @Test
  public void test_two_partitions_multiple_records() {
    send(0,
            new TopicOffset(0, 100),
            new TopicOffset(1, 200),
            new TopicOffset(2, 311)
    );

    Assert.assertNotNull(consumer0.poll()); // waits for async publish

    send(0,
            new TopicOffset(0, 101),
            new TopicOffset(1, 201),
            new TopicOffset(2, 312)
    );

    Assert.assertNotNull(consumer0.poll()); // waits for async publish

    send(1,
            new TopicOffset(0, 1000)
    );

    Assert.assertNotNull(consumer1.poll()); // waits for async publish

    send(1,
            new TopicOffset(0, 1001)
    );

    Assert.assertNotNull(consumer1.poll()); // waits for async publish

    Assignment<String, String> assignment = recoveryService.recover(Arrays.asList(PARTITION_0, PARTITION_1));
    Assert.assertEquals(4, assignment.getOffsets().size());
    Assert.assertEquals(101, (long)assignment.getOffsets().get(new PartitionTopicIdx(0, 0)));
    Assert.assertEquals(201, (long)assignment.getOffsets().get(new PartitionTopicIdx(0, 1)));
    Assert.assertEquals(312, (long)assignment.getOffsets().get(new PartitionTopicIdx(0, 2)));
    Assert.assertEquals(1001, (long)assignment.getOffsets().get(new PartitionTopicIdx(1, 0)));
  }

  private void send(int partition, TopicOffset... topicOffsets) {
    RecordHeaders headers = new RecordHeaders();
    for(TopicOffset topicOffset : topicOffsets) {
      headers.add(RecordHeaderKeys.HEADER_KEY_DEDUP_OFFSET_PREFIX + topicOffset.topicIdx, BitUtil.longToBytes(topicOffset.offset));
    }
    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, partition, null, "", "", headers);
    producer.send(record);
  }

  private static class TopicOffset {
    private final int topicIdx;
    private final long offset;

    public TopicOffset(int topicIdx, long offset) {
      this.topicIdx = topicIdx;
      this.offset = offset;
    }
  }
}
