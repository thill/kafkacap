/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup;

import io.thill.kafkacap.core.dedup.cache.MemoryRecordCache;
import io.thill.kafkacap.core.dedup.callback.DedupStatTracker;
import io.thill.kafkacap.core.dedup.config.DeduplicatorConfig;
import io.thill.kafkacap.core.dedup.handler.RecordHandler;
import io.thill.kafkacap.core.dedup.inbound.FollowConsumer;
import io.thill.kafkacap.core.dedup.inbound.LeadConsumer;
import io.thill.kafkacap.core.dedup.inbound.ThrottledDequeuer;
import io.thill.kafkacap.core.dedup.recovery.RecoveryService;
import io.thill.kafkacap.core.dedup.strategy.DedupStrategy;
import io.thill.kafkacap.core.util.clock.Clock;
import io.thill.kafkacap.core.util.clock.SystemMillisClock;
import io.thill.kafkacap.core.util.io.ResourceLoader;
import io.thill.trakrj.Stats;
import io.thill.trakrj.TrackerId;
import io.thill.trakrj.logger.Slf4jStatLogger;
import org.agrona.concurrent.SigInt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A runnable process that deduplicates inbound Kafka topics into a single outbound topic. The inbound topics are usually populated using a {@link
 * io.thill.kafkacap.core.capture.CaptureDevice}
 *
 * @param <K> Kafka record key type
 * @param <V> Kafka record value type
 * @author Eric Thill
 */
public class Deduplicator<K, V> implements AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ThrottledDequeuer throttledDequeuer;
  private final List<FollowConsumer<K, V>> followConsumers;
  private final LeadConsumer<K, V> leadConsumer;
  private final RecordHandler<K, V> recordHandler;

  Deduplicator(final String consumerGroupIdPrefix,
               final Properties consumerProperties,
               final List<String> topics,
               final RecordHandler<K, V> recordHandler,
               final RecoveryService recoveryService,
               final long manualCommitIntervalMs) {
    throttledDequeuer = new ThrottledDequeuer(recordHandler);
    followConsumers = new ArrayList<>();
    for(int i = 1; i < topics.size(); i++) {
      followConsumers.add(new FollowConsumer<>(createConsumerProperties(consumerGroupIdPrefix, i, consumerProperties), topics.get(i), i, recordHandler, manualCommitIntervalMs));
    }
    leadConsumer = new LeadConsumer<>(createConsumerProperties(consumerGroupIdPrefix, 0, consumerProperties),
            topics.get(0), 0, recordHandler, followConsumers, throttledDequeuer, recoveryService, manualCommitIntervalMs);
    this.recordHandler = recordHandler;
  }

  private static Properties createConsumerProperties(String consumerGroupIdPrefix, int topicIdx, Properties baseProperties) {
    Properties properties = new Properties();
    properties.putAll(baseProperties);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupIdPrefix + topicIdx);
    return properties;
  }

  /**
   * Start the underlying consumers/handlers/dequeuer
   */
  public void start() {
    logger.info("Starting {}", recordHandler);
    recordHandler.start();

    logger.info("Starting {}", throttledDequeuer);
    throttledDequeuer.start();

    for(FollowConsumer<K, V> followConsumer : followConsumers) {
      logger.info("Starting {}", followConsumer);
      followConsumer.start();
    }

    logger.info("Starting {}", leadConsumer);
    leadConsumer.start();

    logger.info("Started");
  }

  @Override
  public void close() {
    logger.info("Closing...");

    logger.info("Closing {}", leadConsumer);
    tryClose(leadConsumer);

    for(FollowConsumer<K, V> followConsumer : followConsumers) {
      logger.info("Closing {}", followConsumer);
      tryClose(followConsumer);
    }

    logger.info("Closing {}", throttledDequeuer);
    tryClose(throttledDequeuer);

    logger.info("Closing {}", recordHandler);
    tryClose(recordHandler);

    logger.info("Close Complete");
  }

  private void tryClose(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch(Throwable t) {
      logger.error("Exception closing " + closeable, t);
    }
  }

  /**
   * Check if the current state of the lead consumer is subscribed
   *
   * @return true if subscribed to partitions, false otherwise
   */
  public boolean isSubscribed() {
    return leadConsumer.isSubscribed();
  }

  /**
   * Main method to start a {@link Deduplicator}
   *
   * @param args Main arguments. Assumes exactly 1 argument which is the path to the configuration file/resource
   * @throws Exception
   */
  public static void main(String... args) throws Exception {
    final Logger logger = LoggerFactory.getLogger(Deduplicator.class);

    // check args
    if(args.length != 1) {
      System.err.println("Usage: Deduplicator <config>");
      logger.error("Missing Configuration Parameter");
      System.exit(1);
    }

    // load config
    logger.info("Loading config from {}...", args[0]);
    final String configStr = ResourceLoader.loadResourceOrFile(args[0]);
    logger.info("Loaded Config:\n{}", configStr);
    final DeduplicatorConfig config = new Yaml().loadAs(configStr, DeduplicatorConfig.class);
    logger.info("Parsed Config: {}", config);

    // start stats
    logger.info("Starting Stats...");
    final Stats stats = Stats.create(new Slf4jStatLogger());

    // instantiate dedup strategy
    logger.info("Instantiating DedupStrategy {}", config.getDedupStrategy().getImpl());
    Class<?> dedupStrategyClass = Class.forName(config.getDedupStrategy().getImpl());
    DedupStrategy dedupStrategy;
    try {
      Constructor<?> dedupStreategyConstructor = dedupStrategyClass.getDeclaredConstructor(Map.class);
      logger.info("Constructing DedupStrategy with props={}", config.getDedupStrategy().getProps());
      dedupStrategy = (DedupStrategy)dedupStreategyConstructor.newInstance(config.getDedupStrategy().getProps());
    } catch(NoSuchMethodException e) {
      logger.info("Constructing DedupStrategy using default constructor");
      dedupStrategy = (DedupStrategy)dedupStrategyClass.newInstance();
    }

    // instantiate and start deduplicator
    logger.info("Instantiating {}...", Deduplicator.class.getSimpleName());
    final Clock clock = new SystemMillisClock();
    final Deduplicator deduplicator = new DeduplicatorBuilder<>()
            .consumerGroupIdPrefix(config.getConsumerGroupIdPrefix())
            .consumerProperties(config.getConsumerProperties())
            .producerProperties(config.getProducerProperties())
            .outboundTopic(config.getOutboundTopic())
            .inboundTopics(config.getInboundTopics())
            .dedupStrategy(dedupStrategy)
            .recordCacheFactory(MemoryRecordCache.factory())
            .orderedCapture(config.isOrderedCapture())
            .dedupCompleteListener(new DedupStatTracker<>(clock, stats, TrackerId.generate("latency"), 10))
            .clock(clock)
            .build();
    logger.info("Registering SigInt Handler...");
    SigInt.register(() -> {
      try {
        logger.info("Closing Deduplicator...");
        deduplicator.close();
        logger.info("Closing Stats...");
        stats.close();
        logger.info("Closed");
      } catch(Throwable t) {
        logger.error("Close Exception", t);
      }
    });
    logger.info("Starting...");
    deduplicator.start();
  }
}
