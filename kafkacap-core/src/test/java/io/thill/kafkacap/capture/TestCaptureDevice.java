package io.thill.kafkacap.capture;

import io.thill.kafkalite.KafkaLite;
import io.thill.kafkalite.client.QueuedKafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCaptureDevice {

  private static final String TOPIC = "TestCaptureDevice";
  private static final int NUM_PARTITIONS = 1;
  private static final int PARTITION = 0;

  private QueueCaptureDevice queueCaptureDevice;
  private QueuedKafkaConsumer<byte[], byte[]> consumer;

  @Before
  public void setup() throws Exception {
    KafkaLite.reset();
    KafkaLite.cleanOnShutdown();
    KafkaLite.createTopic(TOPIC, NUM_PARTITIONS);
    queueCaptureDevice = new QueueCaptureDevice(TOPIC, PARTITION);
    queueCaptureDevice.start();
    consumer = new QueuedKafkaConsumer<>(new TopicPartition(TOPIC, PARTITION), KafkaLite.consumerProperties(ByteArrayDeserializer.class, ByteArrayDeserializer.class));
  }

  @After
  public void teardown() throws Exception {
    if(queueCaptureDevice != null) {
      queueCaptureDevice.close();
      queueCaptureDevice = null;
    }
    if(consumer != null) {
      consumer.close();
      consumer = null;
    }
  }

  @Test
  public void test_multiple_messages() {
    queueCaptureDevice.add("M1".getBytes());
    queueCaptureDevice.add("M2".getBytes());
    queueCaptureDevice.add("M3".getBytes());
    Assert.assertEquals("M1", new String(consumer.poll().value()));
    Assert.assertEquals("M2", new String(consumer.poll().value()));
    Assert.assertEquals("M3", new String(consumer.poll().value()));
    Assert.assertTrue(consumer.isEmpty());
  }
}
