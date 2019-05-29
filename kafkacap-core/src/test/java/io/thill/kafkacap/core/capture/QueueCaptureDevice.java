/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.capture;

import com.google.common.io.Files;
import io.thill.kafkacap.core.capture.config.CaptureDeviceConfig;
import io.thill.kafkacap.core.capture.config.ChronicleConfig;
import io.thill.kafkacap.core.capture.config.KafkaConfig;
import io.thill.kafkacap.core.capture.populator.DefaultRecordPopulator;
import io.thill.kafkacap.core.capture.populator.RecordPopulator;
import io.thill.kafkacap.core.util.clock.Clock;
import io.thill.kafkalite.KafkaLite;
import io.thill.trakrj.Stats;
import io.thill.trakrj.logger.Slf4jStatLogger;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueCaptureDevice extends CaptureDevice<byte[], byte[]> {

  private final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();

  public QueueCaptureDevice(String topic, int partition) {
    super(config(topic, partition), Stats.create(new Slf4jStatLogger()));
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
  protected RecordPopulator<byte[], byte[]> createRecordPopulator(String topic, int partition, Clock clock) {
    return new DefaultRecordPopulator<>(topic, partition, clock);
  }

  @Override
  protected boolean doWork() {
    byte[] payload = queue.poll();
    if(payload == null)
      return false;
    bufferedPublisher.write(payload, 0, payload.length);
    return true;
  }

  @Override
  protected void init() {

  }

  @Override
  protected void cleanup() {
    queue.clear();
  }

  @Override
  protected void onClose() {

  }

}
