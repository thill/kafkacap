package io.thill.kafkacap.capture;

import io.thill.kafkacap.capture.populator.DefaultRecordPopulator;
import io.thill.kafkacap.util.constant.RecordHeaderKeys;
import io.thill.kafkacap.util.clock.SettableClock;
import io.thill.kafkacap.util.io.FileUtil;
import io.thill.kafkalite.KafkaLite;
import io.thill.kafkalite.client.QueuedKafkaConsumer;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TestMulticastCaptureDevice {

  private static final String TOPIC = "TestMulticastCaptureDevice";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
  private static final String CHRONICLE_QUEUE_PATH = "/tmp/TestMulticastCaptureDevice";
  private static final String MULTICAST_ADDRESS = "FF02:0:0:0:0:0:0:1";
  private static final int MULTICAST_PORT = 60137;

  private final SettableClock enqueueClock = new SettableClock();
  private final SettableClock populaterClock = new SettableClock();

  private MulticastCaptureDevice device;
  private QueuedKafkaConsumer<Void, String> kafkaConsumer;
  private MulticastSocket sendSocket;

  @Before
  public void setup() throws Exception {
    KafkaLite.cleanOnShutdown();
    KafkaLite.reset();
    KafkaLite.createTopic(TOPIC, 1);
    kafkaConsumer = new QueuedKafkaConsumer<>(TOPIC_PARTITION, KafkaLite.consumerProperties(ByteArrayDeserializer.class, StringDeserializer.class));
  }

  @After
  public void teardown() throws Exception {
    stop();
    kafkaConsumer.close();
  }

  private void start() throws Exception {
    FileUtil.deleteRecursive(new File(CHRONICLE_QUEUE_PATH));
    BufferedPublisher bufferedPublisher = new BufferedPublisherBuilder()
            .chronicleQueuePath(CHRONICLE_QUEUE_PATH)
            .chronicleQueueRollCycle(RollCycles.TEST_SECONDLY)
            .clock(enqueueClock)
            .kafkaProducerProperties(KafkaLite.producerProperties(ByteArraySerializer.class, ByteArraySerializer.class))
            .recordPopulator(new DefaultRecordPopulator(TOPIC, 0, populaterClock))
            .build();
    MulticastCaptureDevice device = new MulticastCaptureDevice(InetAddress.getLocalHost(), InetAddress.getByName(MULTICAST_ADDRESS), MULTICAST_PORT, 1500, bufferedPublisher);
    device.start();

    sendSocket = new MulticastSocket();
    sendSocket.setInterface(InetAddress.getLocalHost());
    sendSocket.setTimeToLive(1);
  }

  private void stop() throws Exception {
    if(device != null) {
      device.close();
      device = null;
    }
    if(sendSocket != null) {
      sendSocket.close();
      sendSocket = null;
    }
  }

  private long parseLongHeader(ConsumerRecord<?, ?> record, String headerKey) {
    return ByteBuffer.wrap(record.headers().lastHeader(headerKey).value()).order(ByteOrder.LITTLE_ENDIAN).getLong();
  }

  @Test
  public void testMessageFlow() throws Exception {
    start();

    populaterClock.set(3);

    enqueueClock.set(1);
    send("M1".getBytes());

    ConsumerRecord<Void, String> record1 = kafkaConsumer.poll();
    Assert.assertEquals("M1", record1.value());
    Assert.assertEquals(1, parseLongHeader(record1, RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME));
    Assert.assertEquals(3, parseLongHeader(record1, RecordHeaderKeys.HEADER_KEY_CAPTURE_SEND_TIME));

    enqueueClock.set(2);
    send("M2".getBytes());

    ConsumerRecord<Void, String> record2 = kafkaConsumer.poll();
    Assert.assertEquals("M2", record2.value());
    Assert.assertEquals(2, parseLongHeader(record2, RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME));
    Assert.assertEquals(3, parseLongHeader(record2, RecordHeaderKeys.HEADER_KEY_CAPTURE_SEND_TIME));

    Assert.assertTrue(kafkaConsumer.isEmpty());
  }

  private void send(byte[] buf) throws IOException {
    sendSocket.send(new DatagramPacket(buf, buf.length, InetAddress.getByName(MULTICAST_ADDRESS), MULTICAST_PORT));
  }
}
