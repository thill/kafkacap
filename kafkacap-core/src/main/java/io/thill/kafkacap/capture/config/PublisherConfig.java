package io.thill.kafkacap.capture.config;

public class PublisherConfig {
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
    return "PublisherConfig{" +
            "chronicle=" + chronicle +
            ", kafka=" + kafka +
            '}';
  }
}
