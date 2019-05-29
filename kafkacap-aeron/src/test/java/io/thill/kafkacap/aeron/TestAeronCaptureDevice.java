/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.protocol.DataHeaderFlyweight;
import io.thill.kafkacap.aeron.config.AeronCaptureDeviceConfig;
import io.thill.kafkacap.aeron.config.AeronReceiverConfig;
import io.thill.kafkacap.core.capture.config.ChronicleConfig;
import io.thill.kafkacap.core.capture.config.KafkaConfig;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * TestAeronCaptureDevice
 *
 * @author Eric Thill
 */
public class TestAeronCaptureDevice {

  private static final String CHANNEL = "aeron:udp?endpoint=localhost:40123";
  private static final int STREAM_ID = 1;
  private static final String TOPIC = "TestAeronCaptureDevice";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private File aeronDirectory;
  private File chronicleQueuePath;
  private MediaDriver.Context driverContext;
  private MediaDriver driver;
  private AeronCaptureDevice captureDevice;
  private Map<String, TestablePublication> publications = new LinkedHashMap<>();
  private QueuedKafkaConsumer<byte[], String> kafkaConsumer;
  private Stats stats;

  @Before
  public void setup() throws Exception {
    logger.info("=== Setup Start ===");

    // setup temp directories
    aeronDirectory = Files.createTempDirectory("aeron-test-").toFile();
    logger.info("AeronDirectory: {}", aeronDirectory.getAbsolutePath());
    chronicleQueuePath = Files.createTempDirectory("chronicle-test-").toFile();
    logger.info("ChronicleQueuePath: {}", chronicleQueuePath.getAbsolutePath());

    // start embedded driver
    logger.info("Launching Aeron Embedded MediaDriver");
    driverContext = new MediaDriver.Context();
    driverContext.aeronDirectoryName(aeronDirectory.getAbsolutePath());
    driver = MediaDriver.launchEmbedded(driverContext);

    // start kafka
    logger.info("Starting Kafka");
    KafkaLite.cleanOnShutdown();
    KafkaLite.reset();
    KafkaLite.createTopic(TOPIC, 1);

    // create config
    AeronReceiverConfig receiver = new AeronReceiverConfig();
    receiver.setAeronDirectoryName(aeronDirectory.getAbsolutePath());
    receiver.setChannel(CHANNEL);
    receiver.setStreamId(STREAM_ID);

    ChronicleConfig chronicle = new ChronicleConfig();
    chronicle.setPath(chronicleQueuePath.getAbsolutePath());
    chronicle.setRollCycle(RollCycles.TEST_SECONDLY);

    KafkaConfig kafka = new KafkaConfig();
    kafka.setProducerProperties(KafkaLite.producerProperties(ByteArraySerializer.class, ByteArraySerializer.class));
    kafka.setTopic(TOPIC);
    kafka.setPartition(0);

    AeronCaptureDeviceConfig config = new AeronCaptureDeviceConfig();
    config.setReceiver(receiver);
    config.setChronicle(chronicle);
    config.setKafka(kafka);

    logger.info("Starting Kafka Consumer");
    kafkaConsumer = new QueuedKafkaConsumer<>(TOPIC_PARTITION, KafkaLite.consumerProperties(ByteArrayDeserializer.class, StringDeserializer.class));

    logger.info("Starting AeronCaptureDevice");
    stats = Stats.create(new Slf4jStatLogger());
    AeronCaptureDevice captureDevice = new AeronCaptureDevice(config, stats));
    captureDevice.start();
    while(!captureDevice.isStarted())
      Thread.sleep(10);

    logger.info("=== Setup Complete ===");
  }

  @After
  public void teardown() {
    logger.info("=== Teardown Start ===");
    captureDevice = tryClose(captureDevice);
    kafkaConsumer = tryClose(kafkaConsumer);
    for(String name : publications.keySet()) {
      logger.info("Closing Publication {}", name);
      tryClose(publications.get(name));
    }
    publications.clear();
    driver = tryClose(driver);
    aeronDirectory = delete(aeronDirectory);
    chronicleQueuePath = delete(chronicleQueuePath);
    stats.close();
    logger.info("=== Teardown Complete ===");
  }

  private <T extends AutoCloseable> T tryClose(T closeable) {
    if(closeable == null)
      return null;
    try {
      logger.info("Closing {}", closeable.getClass().getSimpleName());
      closeable.close();
    } catch(Throwable t) {
      logger.error("Could not close {}", closeable.getClass().getSimpleName());
    }
    return null;
  }

  private File delete(File file) {
    if(file != null) {
      logger.info("Deleting {}", file.getAbsolutePath());
      FileUtil.deleteRecursive(file);
    }
    return null;
  }

  private void send(String publicationName, String message) throws IOException {
    TestablePublication publication = publications.get(publicationName);
    if(publication == null) {
      logger.info("Starting Publication {}", publicationName);
      publication = new TestablePublication(CHANNEL, STREAM_ID);
      publication.start();
      publications.put(publicationName, publication);
    }
    publication.send(message);
  }

  @Test
  public void testSingleImage() throws Exception {
    send("P1", "M1");
    send("P1", "M2");
    send("P1", "M3");

    ConsumerRecord<byte[], String> record;

    record = kafkaConsumer.poll();
    Assert.assertEquals("M1", record.value());

    record = kafkaConsumer.poll();
    Assert.assertEquals("M2", record.value());

    record = kafkaConsumer.poll();
    Assert.assertEquals("M3", record.value());
  }

  private DataHeaderFlyweight header(byte[] headerBytes) {
    DataHeaderFlyweight header = new DataHeaderFlyweight();
    header.wrap(headerBytes);
    return header;
  }

  @Test
  public void testMultipleImages() throws Exception {
    send("P1", "M1");
    send("P1", "M2");
    send("P2", "M3");
    send("P2", "M4");

    final int p1SessionId = publications.get("P1").sessionId();
    final int p2SessionId = publications.get("P2").sessionId();
    Assert.assertNotEquals(p1SessionId, p2SessionId);

    ConsumerRecord<byte[], String> record;

    record = kafkaConsumer.poll();
    Assert.assertEquals("M1", record.value());
    Assert.assertEquals(p1SessionId, header(record.key()).sessionId());

    record = kafkaConsumer.poll();
    Assert.assertEquals("M2", record.value());
    Assert.assertEquals(p1SessionId, header(record.key()).sessionId());

    record = kafkaConsumer.poll();
    Assert.assertEquals("M3", record.value());
    Assert.assertEquals(p2SessionId, header(record.key()).sessionId());

    record = kafkaConsumer.poll();
    Assert.assertEquals("M4", record.value());
    Assert.assertEquals(p2SessionId, header(record.key()).sessionId());
  }

}
