/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import io.thill.kafkacap.core.dedup.cache.MemoryRecordCache;
import io.thill.kafkacap.core.dedup.cache.RecordCacheFactory;
import io.thill.kafkacap.core.dedup.cache.RecordCacheManager;
import io.thill.kafkacap.core.dedup.callback.DedupCompleteListener;
import io.thill.kafkacap.core.dedup.handler.DisruptorRecordHandler;
import io.thill.kafkacap.core.dedup.handler.RecordHandler;
import io.thill.kafkacap.core.dedup.handler.SingleThreadRecordHandler;
import io.thill.kafkacap.core.dedup.handler.SynchronizedRecordHandler;
import io.thill.kafkacap.core.dedup.outbound.KafkaRecordSender;
import io.thill.kafkacap.core.dedup.outbound.RecordSender;
import io.thill.kafkacap.core.dedup.recovery.LastRecordRecoveryService;
import io.thill.kafkacap.core.dedup.recovery.RecoveryService;
import io.thill.kafkacap.core.dedup.strategy.DedupStrategy;
import io.thill.kafkacap.core.util.clock.Clock;
import io.thill.kafkacap.core.util.clock.SystemMillisClock;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;
import java.util.Properties;

/**
 * Build a {@link Deduplicator}
 *
 * @param <K> Kafka record key type
 * @param <V> Kafka record value type
 */
public class DeduplicatorBuilder<K, V> {

  private String consumerGroupIdPrefix;
  private Properties consumerProperties;
  private Properties producerProperties;
  private List<String> inboundTopics;
  private String outboundTopic;
  private DedupStrategy<K, V> dedupStrategy;
  private RecordCacheFactory<K, V> recordCacheFactory;
  private boolean orderedCapture;
  private Clock clock = new SystemMillisClock();
  private DedupCompleteListener<K, V> dedupCompleteListener;
  private ConcurrencyMode concurrencyMode = ConcurrencyMode.DISRUPTOR;
  private int disruptorRingBufferSize = 1024;
  private WaitStrategy disruptorWaitStrategy;
  private long manualCommitIntervalMs;

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
  public DeduplicatorBuilder<K, V> inboundTopics(List<String> inboundTopics) {
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
   * Set the {@link RecordCacheFactory}. Defaults to {@link MemoryRecordCache#factory()}.
   *
   * @param recordCacheFactory
   * @return
   */
  public DeduplicatorBuilder<K, V> recordCacheFactory(RecordCacheFactory<K, V> recordCacheFactory) {
    this.recordCacheFactory = recordCacheFactory;
    return this;
  }

  /**
   * Set the flag to determine if capture topic is ordered. Defaults to false. Setting to true for capture streams with guaranteed ordering will improve
   * performance.
   *
   * @param orderedCapture
   * @return
   */
  public DeduplicatorBuilder<K, V> orderedCapture(boolean orderedCapture) {
    this.orderedCapture = orderedCapture;
    return this;
  }

  /**
   * The clock used for latency stats tracking. Defaults to {@link SystemMillisClock}
   *
   * @param clock
   * @return
   */
  public DeduplicatorBuilder<K, V> clock(Clock clock) {
    this.clock = clock;
    return this;
  }

  /**
   * Optional. Listener to fire events after a {@link org.apache.kafka.clients.producer.ProducerRecord} has been dispatched to the {@link KafkaProducer}
   *
   * @param dedupCompleteListener
   * @return
   */
  public DeduplicatorBuilder<K, V> dedupCompleteListener(DedupCompleteListener<K, V> dedupCompleteListener) {
    this.dedupCompleteListener = dedupCompleteListener;
    return this;
  }

  /**
   * Set the concurrency mode. Defaults to {@link ConcurrencyMode#DISRUPTOR}.
   *
   * @param concurrencyMode
   * @return
   */
  public DeduplicatorBuilder<K, V> concurrencyMode(ConcurrencyMode concurrencyMode) {
    this.concurrencyMode = concurrencyMode;
    return this;
  }

  /**
   * Set the disruptor ringBuffer size. Defaults to 1024.
   *
   * @param disruptorRingBufferSize
   * @return
   */
  public DeduplicatorBuilder<K, V> disruptorRingBufferSize(int disruptorRingBufferSize) {
    this.disruptorRingBufferSize = disruptorRingBufferSize;
    return this;
  }

  /**
   * Set the disruptor {@link WaitStrategy}. Defaults to {@link BlockingWaitStrategy}.
   *
   * @param disruptorWaitStrategy
   * @return
   */
  public DeduplicatorBuilder<K, V> disruptorWaitStrategy(WaitStrategy disruptorWaitStrategy) {
    this.disruptorWaitStrategy = disruptorWaitStrategy;
    return this;
  }

  /**
   * Set the manual commit interval in milliseconds. Defaults to 0, which means disabled. Setting this to non-zero will periodically flush internal buffers and
   * the producer prior to manually calling consumer.commitAsync()
   *
   * @param manualCommitIntervalMs
   * @return
   */
  public DeduplicatorBuilder<K, V> manualCommitIntervalMs(long manualCommitIntervalMs) {
    this.manualCommitIntervalMs = manualCommitIntervalMs;
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
    if(recordCacheFactory == null)
      recordCacheFactory = MemoryRecordCache.factory();
    if(clock == null)
      throw new IllegalArgumentException("clock cannot be null");
    if(concurrencyMode == null)
      throw new IllegalArgumentException("concurrencyMode cannot be null");


    final RecordCacheManager<K, V> recordCacheManager = new RecordCacheManager<>(recordCacheFactory);
    final RecordSender<K, V> sender = new KafkaRecordSender<>(producerProperties, outboundTopic);
    final RecordHandler<K, V> underlyingRecordHandler = new SingleThreadRecordHandler<>(dedupStrategy, recordCacheManager, orderedCapture, sender, clock, dedupCompleteListener);
    final RecoveryService recoveryService = new LastRecordRecoveryService(consumerProperties, outboundTopic, inboundTopics.size());

    RecordHandler<K, V> recordHandler;
    if(concurrencyMode == ConcurrencyMode.DISRUPTOR) {
      if(disruptorWaitStrategy == null) {
        disruptorWaitStrategy = new BlockingWaitStrategy();
      }
      recordHandler = new DisruptorRecordHandler<>(underlyingRecordHandler, disruptorRingBufferSize, disruptorWaitStrategy);
    } else if(concurrencyMode == ConcurrencyMode.SYNCHRONIZED) {
      recordHandler = new SynchronizedRecordHandler<>(underlyingRecordHandler);
    } else {
      throw new IllegalArgumentException("Unrecognized concurrencyMode: " + concurrencyMode);
    }

    return new Deduplicator<>(consumerGroupIdPrefix, consumerProperties, inboundTopics, recordHandler, recoveryService, manualCommitIntervalMs);
  }

  public enum ConcurrencyMode {
    SYNCHRONIZED, DISRUPTOR
  }
}
