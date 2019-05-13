/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.multicast;

import io.thill.kafkalite.KafkaLite;
import org.agrona.concurrent.SigInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.atomic.AtomicBoolean;

public class MulticastDemo {

  private static final Logger LOGGER = LoggerFactory.getLogger(MulticastDemo.class);
  private static final String MULTICAST_ADDRESS = "FF02:0:0:0:0:0:0:1";
  private static final int MULTICAST_PORT = 60137;

  public static void main(String... args) throws Exception {
    // TODO test IDE demo on non-MacOS operating systems

    KafkaLite.reset();
    KafkaLite.cleanOnShutdown();
    KafkaLite.createTopic("capture_topic_1", 1);

    // send multicast message every second
    new Thread(() -> {
      try {
        final MulticastSocket sendSocket = new MulticastSocket();
        sendSocket.setInterface(InetAddress.getLocalHost());
        sendSocket.setTimeToLive(1);
        long sequence = 0;
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        SigInt.register(() -> keepRunning.set(false));
        while(keepRunning.get()) {
          Thread.sleep(1000);
          String message = "Hello World " + (sequence++);
          LOGGER.info("Sending: {}", message);
          byte[] buf = message.getBytes();
          sendSocket.send(new DatagramPacket(buf, buf.length, InetAddress.getByName(MULTICAST_ADDRESS), MULTICAST_PORT));
        }
      } catch(Exception e) {
        LOGGER.error("Multicast Error", e);
      }
    }).start();

    // start capture device using demo.yaml configuration
    MulticastCaptureDevice.main("demo.yaml");
  }
}
