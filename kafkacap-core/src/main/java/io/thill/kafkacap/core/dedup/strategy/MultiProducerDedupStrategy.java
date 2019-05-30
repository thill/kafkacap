/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.strategy;

import io.thill.kafkacap.core.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An abstract {@link DedupStrategy} that manages a separate {@link DedupStrategy} per inbound producer. This is useful for single physical capture streams that
 * consist of multiple logical streams. The implementing class must define the {@link MultiProducerDedupStrategy#parseProducerKey(ConsumerRecord)} method. Each
 * unique producer will be passed to a separate instance of the underlying {@link DedupStrategy}.
 *
 * @author Eric Thill
 */
public abstract class MultiProducerDedupStrategy<K, V> implements DedupStrategy<K, V> {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Map<String, ProducerContext<K, V>> producerContexts = new LinkedHashMap<>();
  private final DedupStrategyFactory<K, V> dedupStrategyFactory;
  private final long producerExpireIntervalMillis;
  private Assignment<K, V> currentAssignment;
  private long nextExpireCheckTime;

  /**
   * Constructor
   *
   * @param dedupStrategyFactory          The factory that will be used to create new instances of underlying {@link DedupStrategy}s per producer.
   * @param producerExpireIntervalSeconds The number of seconds between checking if an inbound producer is stale and should be expired. The {@link
   *                                      DedupStrategy} correlated with any producer that does not receive any messages for an entire interval will be
   *                                      released.
   */
  public MultiProducerDedupStrategy(DedupStrategyFactory<K, V> dedupStrategyFactory, int producerExpireIntervalSeconds) {
    this.dedupStrategyFactory = dedupStrategyFactory;
    this.producerExpireIntervalMillis = TimeUnit.SECONDS.toMillis(producerExpireIntervalSeconds);
    this.nextExpireCheckTime = System.currentTimeMillis() + producerExpireIntervalMillis;
  }

  @Override
  public DedupResult check(ConsumerRecord<K, V> record) {
    final long now = System.currentTimeMillis();
    final String producerKey = parseProducerKey(record);

    // lookup or create producer context
    ProducerContext<K, V> producerContext = producerContexts.get(producerKey);
    if(producerContext == null) {
      logger.info("Creating DedupStrategy for Producer {}", producerKey);
      producerContext = new ProducerContext<>(dedupStrategyFactory.create(producerKey));
      producerContexts.put(producerKey, producerContext);
      producerContext.getDedupStrategy().assigned(currentAssignment);
    }

    // check result for this producer
    producerContext.setLastReceivedTimestamp(now);
    final DedupResult result = producerContext.getDedupStrategy().check(record);

    // check to expire stale producers
    if(now > nextExpireCheckTime) {
      final Iterator<Map.Entry<String, ProducerContext<K, V>>> iterator = producerContexts.entrySet().iterator();
      while(iterator.hasNext()) {
        final Map.Entry<String, ProducerContext<K, V>> entry = iterator.next();
        if(entry.getValue().getLastReceivedTimestamp() < now - producerExpireIntervalMillis) {
          logger.info("Expiring Producer {}", entry.getKey());
          iterator.remove();
        }
      }
      nextExpireCheckTime = now + producerExpireIntervalMillis;
    }

    return result;
  }

  protected abstract String parseProducerKey(ConsumerRecord<K, V> record);

  @Override
  public void assigned(Assignment<K, V> assignment) {
    this.currentAssignment = assignment;
  }

  @Override
  public void revoked() {
    producerContexts.clear();
  }

  /**
   * Factory for creating underlying {@link DedupStrategy}s per producer.
   */
  public interface DedupStrategyFactory<K, V> {
    DedupStrategy<K, V> create(String producerKey);
  }

  private static class ProducerContext<K, V> {
    private final DedupStrategy<K, V> dedupStrategy;
    private long lastReceivedTimestamp;
    public ProducerContext(DedupStrategy<K, V> dedupStrategy) {
      this.dedupStrategy = dedupStrategy;
    }

    public DedupStrategy<K, V> getDedupStrategy() {
      return dedupStrategy;
    }

    public long getLastReceivedTimestamp() {
      return lastReceivedTimestamp;
    }

    public void setLastReceivedTimestamp(long lastReceivedTimestamp) {
      this.lastReceivedTimestamp = lastReceivedTimestamp;
    }
  }
}
