/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.config;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The configuration object used to start a {@link io.thill.kafkacap.core.dedup.Deduplicator} from a configuration
 *
 * @author Eric Thill
 */
public class DeduplicatorConfig {

  private String consumerGroupIdPrefix;
  private Map<String, String> consumer;
  private Map<String, String> producer;
  private List<String> inboundTopics;
  private String outboundTopic;
  private String dedupStrategy;
  private boolean orderedCapture;
  private long manualCommitIntervalMs;

  public String getConsumerGroupIdPrefix() {
    return consumerGroupIdPrefix;
  }

  public void setConsumerGroupIdPrefix(String consumerGroupIdPrefix) {
    this.consumerGroupIdPrefix = consumerGroupIdPrefix;
  }

  public Properties getConsumerProperties() {
    Properties props = new Properties();
    props.putAll(consumer);
    return props;
  }

  public Map<String, String> getConsumer() {
    return consumer;
  }

  public void setConsumer(Map<String, String> consumer) {
    this.consumer = consumer;
  }

  public Properties getProducerProperties() {
    Properties props = new Properties();
    props.putAll(producer);
    return props;
  }

  public Map<String, String> getProducer() {
    return producer;
  }

  public void setProducer(Map<String, String> producer) {
    this.producer = producer;
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

  public boolean isOrderedCapture() {
    return orderedCapture;
  }

  public void setOrderedCapture(boolean orderedCapture) {
    this.orderedCapture = orderedCapture;
  }

  @Override
  public String toString() {
    return "DeduplicatorConfig{" +
            "consumerGroupIdPrefix='" + consumerGroupIdPrefix + '\'' +
            ", consumer=" + consumer +
            ", producer=" + producer +
            ", inboundTopics=" + inboundTopics +
            ", outboundTopic='" + outboundTopic + '\'' +
            ", dedupStrategy='" + dedupStrategy + '\'' +
            ", orderedCapture=" + orderedCapture +
            ", manualCommitIntervalMs=" + manualCommitIntervalMs +
            '}';
  }
}
