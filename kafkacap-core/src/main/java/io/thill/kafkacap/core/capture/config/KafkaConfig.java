/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.capture.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The Kafka portion of a {@link CaptureDeviceConfig}
 *
 * @author Eric Thill
 */
public class KafkaConfig {
  private Map<String, String> producer;
  private String topic;
  private Integer partition;

  public Map<String, String> getProducer() {
    return producer;
  }

  public void setProducer(Map<String, String> producer) {
    this.producer = producer;
  }

  public void setProducerProperties(Properties producer) {
    this.producer = new LinkedHashMap<>();
    for(Map.Entry<Object, Object> e : producer.entrySet()) {
      this.producer.put(e.getKey().toString(), e.getValue().toString());
    }
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public Integer getPartition() {
    return partition;
  }

  public void setPartition(Integer partition) {
    this.partition = partition;
  }

  @Override
  public String toString() {
    return "KafkaConfig{" +
            "producer=" + producer +
            ", topic='" + topic + '\'' +
            ", partition=" + partition +
            '}';
  }
}
