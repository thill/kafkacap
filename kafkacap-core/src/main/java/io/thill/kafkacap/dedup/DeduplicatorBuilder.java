/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import io.thill.kafkacap.dedup.callback.DedupCompleteListener;
import io.thill.kafkacap.dedup.handler.DisruptorRecordHandler;
import io.thill.kafkacap.dedup.handler.RecordHandler;
import io.thill.kafkacap.dedup.handler.SingleThreadRecordHandler;
import io.thill.kafkacap.dedup.handler.SynchronizedRecordHandler;
import io.thill.kafkacap.dedup.outbound.KafkaRecordSender;
import io.thill.kafkacap.dedup.outbound.RecordSender;
import io.thill.kafkacap.dedup.queue.DedupQueue;
import io.thill.kafkacap.dedup.queue.MemoryDedupQueue;
import io.thill.kafkacap.dedup.recovery.LastRecordRecoveryService;
import io.thill.kafkacap.dedup.recovery.RecoveryService;
import io.thill.kafkacap.dedup.strategy.DedupStrategy;
import io.thill.kafkacap.util.clock.Clock;
import io.thill.kafkacap.util.clock.SystemMillisClock;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;
import java.util.Properties;

public class DeduplicatorBuilder<K, V> {

  private String consumerGroupIdPrefix;
  private Properties consumerProperties;
  private Properties producerProperties;
  private List<String> inboundTopics;
  private String outboundTopic;
  private DedupStrategy<K, V> dedupStrategy;
  private DedupQueue<K, V> dedupQueue;
  private Clock clock = new SystemMillisClock();
  private DedupCompleteListener<K, V> dedupCompleteListener;
  private ConcurrencyMode concurrencyMode = ConcurrencyMode.DISRUPTOR;
  private int disruptorRingBufferSize = 1024;
  private WaitStrategy disruptorWaitStrategy;

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
   * Set the {@link DedupQueue}. Defaults to {@link MemoryDedupQueue}.
   *
   * @param dedupQueue
   * @return
   */
  public DeduplicatorBuilder<K, V> dedupQueue(DedupQueue<K, V> dedupQueue) {
    this.dedupQueue = dedupQueue;
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
   * @param disruptorWaitStrategy
   * @return
   */
  public DeduplicatorBuilder<K, V> disruptorWaitStrategy(WaitStrategy disruptorWaitStrategy) {
    this.disruptorWaitStrategy = disruptorWaitStrategy;
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
    if(clock == null)
      throw new IllegalArgumentException("clock cannot be null");
    if(concurrencyMode == null)
      throw new IllegalArgumentException("concurrencyMode cannot be null");


    final RecordSender<K, V> sender = new KafkaRecordSender<>(producerProperties, outboundTopic);
    final RecordHandler<K,V> underlyingRecordHandler = new SingleThreadRecordHandler<>(dedupStrategy, dedupQueue, sender, clock, dedupCompleteListener);
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

    return new Deduplicator<>(consumerGroupIdPrefix, consumerProperties, inboundTopics, recordHandler, recoveryService);
  }

  public enum ConcurrencyMode {
    SYNCHRONIZED, DISRUPTOR
  }
}
