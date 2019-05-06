package io.thill.kafkacap.dedup;

import io.thill.kafkacap.dedup.handler.RecordHandler;
import io.thill.kafkacap.dedup.handler.SynchronizedRecordHandler;
import io.thill.kafkacap.dedup.outbound.KafkaRecordSender;
import io.thill.kafkacap.dedup.outbound.RecordSender;
import io.thill.kafkacap.dedup.queue.DedupQueue;
import io.thill.kafkacap.dedup.queue.MemoryDedupQueue;
import io.thill.kafkacap.dedup.strategy.DedupStrategy;

import java.util.Collection;
import java.util.Properties;

public class DeduplicatorBuilder<K, V> {

  private String consumerGroupIdPrefix;
  private Properties consumerProperties;
  private Properties producerProperties;
  private Collection<String> inboundTopics;
  private String outboundTopic;
  private DedupStrategy<K, V> dedupStrategy;
  private DedupQueue<K, V> dedupQueue;

  /**
   * Set the Kafka Consumer group.id prefix
   *
   * @param consumerGroupIdPrefix
   * @return
   */
  public DeduplicatorBuilder<K, V> consumerGroupIdPrefix(String consumerGroupIdPrefix) {
    this.consumerGroupIdPrefix = consumerGroupIdPrefix;
    return this;
  }

  /**
   * Set the Kafka Consumer Properties
   *
   * @param consumerProperties
   * @return
   */
  public DeduplicatorBuilder<K, V> consumerProperties(Properties consumerProperties) {
    this.consumerProperties = consumerProperties;
    return this;
  }

  /**
   * Set the Kafka Producer Properties
   *
   * @param producerProperties
   * @return
   */
  public DeduplicatorBuilder<K, V> producerProperties(Properties producerProperties) {
    this.producerProperties = producerProperties;
    return this;
  }

  /**
   * Set the Kafka Inbound Topics to deduplicate
   *
   * @param inboundTopics
   * @return
   */
  public DeduplicatorBuilder<K, V> inboundTopics(Collection<String> inboundTopics) {
    this.inboundTopics = inboundTopics;
    return this;
  }

  /**
   * Set the Outbound/Deduplicated topic
   *
   * @param outboundTopic
   * @return
   */
  public DeduplicatorBuilder<K, V> outboundTopic(String outboundTopic) {
    this.outboundTopic = outboundTopic;
    return this;
  }

  /**
   * Set the {@link DedupStrategy}
   *
   * @param dedupStrategy
   * @return
   */
  public DeduplicatorBuilder<K, V> dedupStrategy(DedupStrategy<K, V> dedupStrategy) {
    this.dedupStrategy = dedupStrategy;
    return this;
  }

  /**
   * Set the {@link DedupQueue}. Defaults to {@link MemoryDedupQueue}.
   *
   * @param dedupQueue
   * @return
   */
  public DeduplicatorBuilder<K, V> dedupQueue(DedupQueue<K, V> dedupQueue) {
    this.dedupQueue = dedupQueue;
    return this;
  }

  public Deduplicator<K, V> build() {
    if(consumerGroupIdPrefix == null)
      throw new IllegalArgumentException("consumerGroupIdPrefix cannot be null");
    if(consumerProperties == null)
      throw new IllegalArgumentException("consumerProperties cannot be null");
    if(producerProperties == null)
      throw new IllegalArgumentException("producerProperties cannot be null");
    if(inboundTopics == null || inboundTopics.size() == 0)
      throw new IllegalArgumentException("inboundTopics cannot be null or empty");
    if(outboundTopic == null)
      throw new IllegalArgumentException("outboundTopic cannot be null");
    if(dedupStrategy == null)
      throw new IllegalArgumentException("dedupStrategy cannot be null");
    if(dedupQueue == null)
      dedupQueue = new MemoryDedupQueue<>();

    final RecordSender<K, V> sender = new KafkaRecordSender<>(producerProperties, outboundTopic);
    final RecordHandler<K, V> recordHandler = new SynchronizedRecordHandler<>(dedupStrategy, dedupQueue, sender);
    return new Deduplicator<>(consumerGroupIdPrefix, consumerProperties, inboundTopics, recordHandler, sender);
  }
}
