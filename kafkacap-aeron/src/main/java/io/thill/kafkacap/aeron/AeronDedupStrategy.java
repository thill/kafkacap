/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.aeron;

import io.aeron.protocol.DataHeaderFlyweight;
import io.thill.kafkacap.core.dedup.strategy.MultiProducerDedupStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A {@link MultiProducerDedupStrategy} implementation that uses the Aeron stream ID from the Aeron Header / Kafka Key for the producer ID
 *
 * @author Eric Thill
 */
public class AeronDedupStrategy extends MultiProducerDedupStrategy<byte[], byte[]> {

  private final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();

  /**
   * Constructor
   *
   * @param dedupStrategyFactory         The factory that will be used to create new instances of underlying {@link io.thill.kafkacap.core.dedup.strategy.DedupStrategy}s
   *                                     per sessionID.
   * @param sessionExpireIntervalSeconds The number of seconds between checking if an inbound session is stale and should be expired. The {@link
   *                                     io.thill.kafkacap.core.dedup.strategy.DedupStrategy} correlated with any session that does not receive any messages for
   *                                     an entire interval will be released.
   */
  public AeronDedupStrategy(DedupStrategyFactory<byte[], byte[]> dedupStrategyFactory, int sessionExpireIntervalSeconds) {
    super(dedupStrategyFactory, sessionExpireIntervalSeconds);
  }

  @Override
  protected String parseProducerKey(ConsumerRecord<byte[], byte[]> record) {
    dataHeaderFlyweight.wrap(record.key());
    return Integer.toString(dataHeaderFlyweight.sessionId());
  }

}
