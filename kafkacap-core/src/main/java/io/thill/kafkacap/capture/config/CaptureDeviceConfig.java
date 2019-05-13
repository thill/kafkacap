/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.config;

public class CaptureDeviceConfig {
  private ChronicleConfig chronicle;
  private KafkaConfig kafka;

  public ChronicleConfig getChronicle() {
    return chronicle;
  }

  public void setChronicle(ChronicleConfig chronicle) {
    this.chronicle = chronicle;
  }

  public KafkaConfig getKafka() {
    return kafka;
  }

  public void setKafka(KafkaConfig kafka) {
    this.kafka = kafka;
  }

  @Override
  public String toString() {
    return "CaptureDeviceConfig{" +
            "chronicle=" + chronicle +
            ", kafka=" + kafka +
            '}';
  }
}
