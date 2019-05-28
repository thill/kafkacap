/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.strategy;

import io.thill.kafkacap.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final Map<String, DedupStrategy<K, V>> producerDedupStrategies = new LinkedHashMap<>();
  private final DedupStrategyFactory<K, V> dedupStrategyFactory;
  private final long producerExpireIntervalMillis;
  private Assignment<K, V> currentAssignment;

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
  }

  @Override
  public DedupResult check(ConsumerRecord<K, V> record) {
    final String producerKey = parseProducerKey(record);
    DedupStrategy<K, V> dedupStrategy = producerDedupStrategies.get(producerKey);
    if(dedupStrategy == null) {
      logger.info("Creating DedupStrategy for Producer {}", producerKey);
      dedupStrategy = dedupStrategyFactory.create(producerKey);
      producerDedupStrategies.put(producerKey, dedupStrategy);
      dedupStrategy.assigned(currentAssignment);
    }
    return dedupStrategy.check(record);
  }

  protected abstract String parseProducerKey(ConsumerRecord<K, V> record);

  @Override
  public void assigned(Assignment<K, V> assignment) {
    this.currentAssignment = assignment;
  }

  @Override
  public void revoked() {
    producerDedupStrategies.clear();
  }

  /**
   * Factory for creating underlying {@link DedupStrategy}s per producer.
   */
  public interface DedupStrategyFactory<K, V> {
    DedupStrategy<K, V> create(Object producerKey);
  }
}
