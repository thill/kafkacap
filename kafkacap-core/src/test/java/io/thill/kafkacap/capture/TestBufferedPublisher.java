/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture;

import io.thill.kafkacap.capture.populator.DefaultRecordPopulator;
import io.thill.kafkacap.capture.queue.MemoryCaptureQueue;
import io.thill.kafkacap.util.clock.SettableClock;
import io.thill.kafkacap.util.constant.RecordHeaderKeys;
import io.thill.kafkacap.util.io.FileUtil;
import io.thill.kafkalite.KafkaLite;
import io.thill.kafkalite.client.QueuedKafkaConsumer;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TestBufferedPublisher {

  private static final String TOPIC = "TestBufferedPublisher";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
  private static final String CHRONICLE_QUEUE_PATH = "/tmp/TestBufferedPublisher";

  private final SettableClock enqueueClock = new SettableClock();
  private final SettableClock populaterClock = new SettableClock();

  private BufferedPublisher bufferedPublisher;
  private QueuedKafkaConsumer<String, String> kafkaConsumer;

  @Before
  public void setup() throws Exception {
    KafkaLite.cleanOnShutdown();
    KafkaLite.reset();
    KafkaLite.createTopic(TOPIC, 1);
    kafkaConsumer = new QueuedKafkaConsumer<>(TOPIC_PARTITION, KafkaLite.consumerProperties(StringDeserializer.class, StringDeserializer.class));
  }

  @After
  public void teardown() throws Exception {
    stop();
    kafkaConsumer.close();
  }

  private void start() {
    FileUtil.deleteRecursive(new File(CHRONICLE_QUEUE_PATH));
    bufferedPublisher = new BufferedPublisherBuilder()
            .captureQueue(new MemoryCaptureQueue())
            .clock(enqueueClock)
            .kafkaProducerProperties(KafkaLite.producerProperties(ByteArraySerializer.class, ByteArraySerializer.class))
            .recordPopulator(new DefaultRecordPopulator(TOPIC, 0, populaterClock))
            .build();
    bufferedPublisher.start();
  }

  private void stop() throws Exception {
    if(bufferedPublisher != null) {
      bufferedPublisher.close();
      bufferedPublisher = null;
    }
  }

  private long parseLongHeader(ConsumerRecord<?, ?> record, String headerKey) {
    return ByteBuffer.wrap(record.headers().lastHeader(headerKey).value()).order(ByteOrder.LITTLE_ENDIAN).getLong();
  }

  @Test
  public void testMessageFlow() {
    start();

    populaterClock.set(3);

    enqueueClock.set(1);
    bufferedPublisher.write("M1".getBytes());

    enqueueClock.set(2);
    bufferedPublisher.write("M2".getBytes());

    ConsumerRecord<String, String> record1 = kafkaConsumer.poll();
    Assert.assertEquals("M1", record1.value());
    Assert.assertEquals(1, parseLongHeader(record1, RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME));
    Assert.assertEquals(3, parseLongHeader(record1, RecordHeaderKeys.HEADER_KEY_CAPTURE_SEND_TIME));

    ConsumerRecord<String, String> record2 = kafkaConsumer.poll();
    Assert.assertEquals("M2", record2.value());
    Assert.assertEquals(2, parseLongHeader(record2, RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME));
    Assert.assertEquals(3, parseLongHeader(record2, RecordHeaderKeys.HEADER_KEY_CAPTURE_SEND_TIME));

    Assert.assertTrue(kafkaConsumer.isEmpty());
  }

  @Test
  public void testRestart() throws Exception {
    start();

    enqueueClock.set(1);
    populaterClock.set(3);
    bufferedPublisher.write("M1".getBytes());

    ConsumerRecord<String, String> record1 = kafkaConsumer.poll();
    Assert.assertEquals("M1", record1.value());
    Assert.assertEquals(1, parseLongHeader(record1, RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME));
    Assert.assertEquals(3, parseLongHeader(record1, RecordHeaderKeys.HEADER_KEY_CAPTURE_SEND_TIME));

    Assert.assertTrue(kafkaConsumer.isEmpty());

    stop();
    start();

    enqueueClock.set(4);
    populaterClock.set(5);
    bufferedPublisher.write("M2".getBytes());

    ConsumerRecord<String, String> record2 = kafkaConsumer.poll();
    Assert.assertEquals("M2", record2.value());
    Assert.assertEquals(4, parseLongHeader(record2, RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME));
    Assert.assertEquals(5, parseLongHeader(record2, RecordHeaderKeys.HEADER_KEY_CAPTURE_SEND_TIME));

    Assert.assertTrue(kafkaConsumer.isEmpty());
  }

}
