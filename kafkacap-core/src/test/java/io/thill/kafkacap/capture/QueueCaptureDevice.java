/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture;

import com.google.common.io.Files;
import io.thill.kafkacap.capture.config.CaptureDeviceConfig;
import io.thill.kafkacap.capture.config.ChronicleConfig;
import io.thill.kafkacap.capture.config.KafkaConfig;
import io.thill.kafkalite.KafkaLite;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueCaptureDevice extends CaptureDevice {

  private final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();

  public QueueCaptureDevice(String topic, int partition) {
    super(config(topic, partition));
  }

  private static CaptureDeviceConfig config(String topic, int partition) {
    CaptureDeviceConfig config = new CaptureDeviceConfig();

    ChronicleConfig chronicle = new ChronicleConfig();
    chronicle.setPath(Files.createTempDir().getAbsolutePath());
    chronicle.setRollCycle(RollCycles.TEST_SECONDLY);
    config.setChronicle(chronicle);

    KafkaConfig kafka = new KafkaConfig();
    kafka.setProducerProperties(KafkaLite.producerProperties(ByteArraySerializer.class, ByteArraySerializer.class));
    kafka.setTopic(topic);
    kafka.setPartition(partition);
    config.setKafka(kafka);

    return config;
  }

  public void add(byte[] payload) {
    queue.add(payload);
  }

  @Override
  protected boolean poll(BufferHandler handler) throws Exception {
    byte[] payload = queue.poll();
    if(payload == null)
      return false;
    handler.handle(payload, 0, payload.length);
    return true;
  }

  @Override
  protected void init() throws Exception {

  }

  @Override
  protected void cleanup() {
    queue.clear();
  }

  @Override
  protected void onClose() {

  }
}
