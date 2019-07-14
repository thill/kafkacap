/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.multicast;

import io.thill.kafkacap.core.capture.config.ChronicleConfig;
import io.thill.kafkacap.core.capture.config.KafkaConfig;
import io.thill.kafkacap.core.util.io.BitUtil;
import io.thill.kafkacap.multicast.config.MulticastCaptureDeviceConfig;
import io.thill.kafkacap.multicast.config.MulticastConfig;
import io.thill.kafkacap.core.util.constant.RecordHeaderKeys;
import io.thill.kafkacap.core.util.io.FileUtil;
import io.thill.kafkalite.KafkaLite;
import io.thill.kafkalite.client.QueuedKafkaConsumer;
import io.thill.trakrj.Stats;
import io.thill.trakrj.logger.Slf4jStatLogger;
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
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TestMulticastCaptureDevice {

  private static final String TOPIC = "TestMulticastCaptureDevice";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
  private static final String CHRONICLE_QUEUE_PATH = "/tmp/TestMulticastCaptureDevice";
  private static final String MULTICAST_ADDRESS = "FF02:0:0:0:0:0:0:1";
  private static final int MULTICAST_PORT = 60137;

  private MulticastCaptureDevice device;
  private QueuedKafkaConsumer<Void, String> kafkaConsumer;
  private MulticastSocket sendSocket;
  private Stats stats;

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

    MulticastConfig receiver = new MulticastConfig();
    receiver.setGroup(MULTICAST_ADDRESS);
    receiver.setIface("localhost");
    receiver.setMtu(1500);
    receiver.setPort(MULTICAST_PORT);

    ChronicleConfig chronicle = new ChronicleConfig();
    chronicle.setPath(CHRONICLE_QUEUE_PATH);
    chronicle.setRollCycle(RollCycles.TEST_SECONDLY);

    KafkaConfig kafka = new KafkaConfig();
    kafka.setProducerProperties(KafkaLite.producerProperties(ByteArraySerializer.class, ByteArraySerializer.class));
    kafka.setTopic(TOPIC);
    kafka.setPartition(0);

    MulticastCaptureDeviceConfig mcastConfig = new MulticastCaptureDeviceConfig();
    mcastConfig.setReceiver(receiver);
    mcastConfig.setChronicle(chronicle);
    mcastConfig.setKafka(kafka);

    stats = Stats.create(new Slf4jStatLogger());
    MulticastCaptureDevice device = new MulticastCaptureDevice(mcastConfig, stats);
    device.start();
    while(!device.isStarted())
      Thread.sleep(10);

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
    if(stats != null) {
      stats.close();
    }
  }

  private long parseLongHeader(ConsumerRecord<?, ?> record, String headerKey) {
    return BitUtil.bytesToLong(record.headers().lastHeader(headerKey).value());
  }

  @Test
  public void testMessageFlow() throws Exception {
    start();

    final long time = System.currentTimeMillis();

    send("M1".getBytes());

    ConsumerRecord<Void, String> record1 = kafkaConsumer.poll();
    Assert.assertEquals("M1", record1.value());
    Assert.assertTrue(parseLongHeader(record1, RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME) >= time);
    Assert.assertTrue(parseLongHeader(record1, RecordHeaderKeys.HEADER_KEY_CAPTURE_SEND_TIME) >= time);

    send("M2".getBytes());

    ConsumerRecord<Void, String> record2 = kafkaConsumer.poll();
    Assert.assertEquals("M2", record2.value());
    Assert.assertTrue(parseLongHeader(record2, RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME) >= time);
    Assert.assertTrue(parseLongHeader(record2, RecordHeaderKeys.HEADER_KEY_CAPTURE_SEND_TIME) >= time);

    Assert.assertTrue(kafkaConsumer.isEmpty());
  }

  private void send(byte[] buf) throws IOException {
    sendSocket.send(new DatagramPacket(buf, buf.length, InetAddress.getByName(MULTICAST_ADDRESS), MULTICAST_PORT));
  }
}
