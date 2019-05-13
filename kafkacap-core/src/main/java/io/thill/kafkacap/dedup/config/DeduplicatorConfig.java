package io.thill.kafkacap.dedup.config;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DeduplicatorConfig {

  private String consumerGroupIdPrefix;
  private Map<String, String> consumerProperties;
  private Map<String, String> producerProperties;
  private List<String> inboundTopics;
  private String outboundTopic;
  private String dedupStrategy;

  public String getConsumerGroupIdPrefix() {
    return consumerGroupIdPrefix;
  }

  public void setConsumerGroupIdPrefix(String consumerGroupIdPrefix) {
    this.consumerGroupIdPrefix = consumerGroupIdPrefix;
  }

  public Properties getConsumerProperties() {
    Properties props = new Properties();
    props.putAll(consumerProperties);
    return props;
  }

  public void setConsumerProperties(Map<String, String> consumerProperties) {
    this.consumerProperties = consumerProperties;
  }

  public Properties getProducerProperties() {
    Properties props = new Properties();
    props.putAll(producerProperties);
    return props;
  }

  public void setProducerProperties(Map<String, String> producerProperties) {
    this.producerProperties = producerProperties;
  }

  public List<String> getInboundTopics() {
    return inboundTopics;
  }

  public void setInboundTopics(List<String> inboundTopics) {
    this.inboundTopics = inboundTopics;
  }

  public String getOutboundTopic() {
    return outboundTopic;
  }

  public void setOutboundTopic(String outboundTopic) {
    this.outboundTopic = outboundTopic;
  }

  public String getDedupStrategy() {
    return dedupStrategy;
  }

  public void setDedupStrategy(String dedupStrategy) {
    this.dedupStrategy = dedupStrategy;
  }

  @Override
  public String toString() {
    return "DeduplicatorConfig{" +
            "consumerGroupIdPrefix='" + consumerGroupIdPrefix + '\'' +
            ", consumerProperties=" + consumerProperties +
            ", producerProperties=" + producerProperties +
            ", inboundTopics=" + inboundTopics +
            ", outboundTopic='" + outboundTopic + '\'' +
            ", dedupStrategy='" + dedupStrategy + '\'' +
            '}';
  }
}
