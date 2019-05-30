/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.websocket;

import io.thill.kafkacap.core.capture.config.ChronicleConfig;
import io.thill.kafkacap.core.capture.config.KafkaConfig;
import io.thill.kafkacap.websocket.config.WebsocketCaptureDeviceConfig;
import io.thill.kafkacap.websocket.config.WebsocketConfig;
import io.thill.kafkalite.KafkaLite;
import io.thill.trakrj.Stats;
import io.thill.trakrj.logger.Slf4jStatLogger;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;

/**
 * WebsocketDemo
 *
 * @author Eric Thill
 */
public class WebsocketDemo {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketDemo.class);
  private static final String TOPIC = "TestMulticastCaptureDevice";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);

  public static void main(String... args) throws Exception {
    if(args.length == 0) {
      System.err.println("Use: WebsocketDemo <url>");
      return;
    }

    final String url = args[0];

    // setup kafka and chronicle
    KafkaLite.cleanOnShutdown();
    KafkaLite.reset();
    KafkaLite.createTopic(TOPIC, 1);
    File chronicleQueueDir = Files.createTempDirectory("chronicle").toFile();

    // create the configuration
    WebsocketConfig receiver = new WebsocketConfig();
    receiver.setUrl(url);

    ChronicleConfig chronicle = new ChronicleConfig();
    chronicle.setPath(chronicleQueueDir.getAbsolutePath());
    chronicle.setRollCycle(RollCycles.MINUTELY);

    KafkaConfig kafka = new KafkaConfig();
    kafka.setProducerProperties(KafkaLite.producerProperties(ByteArraySerializer.class, ByteArraySerializer.class));
    kafka.setTopic(TOPIC);
    kafka.setPartition(0);

    WebsocketCaptureDeviceConfig config = new WebsocketCaptureDeviceConfig();
    config.setReceiver(receiver);
    config.setChronicle(chronicle);
    config.setKafka(kafka);

    // create and start WebsocketCaptureDevice
    Stats stats = Stats.create(new Slf4jStatLogger());
    WebsocketCaptureDevice device = new WebsocketCaptureDevice(config, stats);
    device.start();
    while(!device.isStarted())
      Thread.sleep(10);

    // poll kafka topic for captured messages, log them
    KafkaConsumer<byte[], String> kafkaConsumer = new KafkaConsumer<byte[], String>(KafkaLite.consumerProperties(ByteArrayDeserializer.class, StringDeserializer.class));
    kafkaConsumer.assign(Arrays.asList(TOPIC_PARTITION));
    while(true) {
      for(ConsumerRecord<byte[], String> r : kafkaConsumer.poll(Duration.ofSeconds(1))) {
        LOGGER.info("Captured: " + r.value());
      }
    }
  }

}
