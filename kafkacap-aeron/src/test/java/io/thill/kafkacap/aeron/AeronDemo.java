/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.aeron;

import io.aeron.driver.MediaDriver;
import io.thill.kafkacap.core.util.io.FileUtil;
import io.thill.kafkalite.KafkaLite;
import org.agrona.concurrent.SigInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

public class AeronDemo {

  private static final Logger LOGGER = LoggerFactory.getLogger(AeronDemo.class);
  private static final String AERON_DIRECTORY = "/tmp/aeron-media-driver";
  private static final String CHANNEL = "aeron:udp?endpoint=localhost:40123";
  private static final int STREAM_ID = 1;

  public static void main(String... args) throws Exception {

    KafkaLite.cleanOnShutdown();
    KafkaLite.reset();
    KafkaLite.createTopic("capture_topic_1", 1);
    SigInt.register(() -> KafkaLite.stop());

    FileUtil.deleteRecursive(new File(AERON_DIRECTORY));
    MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
    mediaDriverContext.aeronDirectoryName(AERON_DIRECTORY);
    final MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverContext);
    SigInt.register(() -> mediaDriver.close());

    // send aeron message every second
    new Thread(() -> {
      final TestablePublication publication = new TestablePublication(CHANNEL, STREAM_ID);
      try {
        publication.start();
        long sequence = 0;
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        SigInt.register(() -> keepRunning.set(false));
        while(keepRunning.get()) {
          Thread.sleep(1000);
          String message = "Hello World " + (sequence++);
          LOGGER.info("Sending: {}", message);
          publication.send(message);
        }
      } catch(Exception e) {
        LOGGER.error("Aeron Error", e);
      } finally {
        publication.close();
      }
    }).start();

    // start capture device using demo.yaml configuration
    AeronCaptureDevice.main("demo.yaml");
  }
}
