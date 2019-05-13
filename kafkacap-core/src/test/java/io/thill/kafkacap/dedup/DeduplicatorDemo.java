/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup;

import io.thill.kafkacap.util.constant.RecordHeaderKeys;
import io.thill.kafkacap.util.io.BitUtil;
import io.thill.kafkalite.KafkaLite;
import org.agrona.concurrent.SigInt;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DeduplicatorDemo
 *
 * @author Eric Thill
 */
public class DeduplicatorDemo {

  public static void main(String... args) throws Exception {
    final Logger logger = LoggerFactory.getLogger(DeduplicatorDemo.class);

    // setup kafka
    KafkaLite.cleanOnShutdown();
    KafkaLite.reset();
    KafkaLite.createTopic("capture_A", 1);
    KafkaLite.createTopic("capture_B", 1);
    KafkaLite.createTopic("outbound", 1);
    SigInt.register(() -> KafkaLite.stop());

    // consumer
    new Thread(() -> {
      try {
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        SigInt.register(() -> keepRunning.set(false));
        final KafkaConsumer<Long, String> kafkaConsumer = new KafkaConsumer<>(KafkaLite.consumerProperties(LongDeserializer.class, StringDeserializer.class));
        kafkaConsumer.assign(Arrays.asList(new TopicPartition("outbound", 0)));
        while(keepRunning.get()) {
          for(ConsumerRecord<Long, String> record : kafkaConsumer.poll(Duration.ofSeconds(1))) {
            logger.info("Received: seq={} value={}", record.key(), record.value());
          }
        }
      } catch(Throwable t) {
        logger.error("Consumer Exception", t);
      }
    }).start();

    // producer
    new Thread(() -> {
      try {
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        SigInt.register(() -> keepRunning.set(false));
        final KafkaProducer<Long, String> producer = new KafkaProducer<>(KafkaLite.producerProperties(LongSerializer.class, StringSerializer.class));
        long seq = 0;
        while(keepRunning.get()) {
          RecordHeaders headers = new RecordHeaders();
          headers.add(RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME, BitUtil.longToBytes(System.currentTimeMillis()));
          producer.send(new ProducerRecord<>("capture_A", 0, null, seq, "Hello World " + seq, headers));
          producer.send(new ProducerRecord<>("capture_B", 0, null, seq, "Hello World " + seq, headers));
          seq++;
          Thread.sleep(500);
        }
      } catch(Throwable t) {
        logger.error("Producer Exception", t);
      }
    }).start();

    // start deduplicator
    Deduplicator.main("dedup_demo.yaml");
  }
}
