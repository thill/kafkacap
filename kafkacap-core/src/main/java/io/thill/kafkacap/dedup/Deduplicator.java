package io.thill.kafkacap.dedup;

import io.thill.kafkacap.dedup.handler.RecordHandler;
import io.thill.kafkacap.dedup.inbound.FollowConsumer;
import io.thill.kafkacap.dedup.inbound.LeadConsumer;
import io.thill.kafkacap.dedup.inbound.ThrottledDequeuer;
import io.thill.kafkacap.dedup.outbound.RecordSender;
import io.thill.kafkacap.dedup.recovery.RecoveryService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Deduplicator<K, V> implements AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ThrottledDequeuer throttledDequeuer;
  private final List<FollowConsumer<K, V>> followConsumers;
  private final LeadConsumer<K, V> leadConsumer;
  private final RecordHandler<K,V> recordHandler;

  public Deduplicator(final String consumerGroupIdPrefix,
                      final Properties consumerProperties,
                      final List<String> topics,
                      final RecordHandler<K, V> recordHandler,
                      final RecoveryService recoveryService) {
    throttledDequeuer = new ThrottledDequeuer(recordHandler);
    followConsumers = new ArrayList<>();
    for(int i = 1; i < topics.size(); i++) {
      followConsumers.add(new FollowConsumer<>(createConsumerProperties(consumerGroupIdPrefix, i, consumerProperties), topics.get(i), i, recordHandler));
    }
    leadConsumer = new LeadConsumer<>(createConsumerProperties(consumerGroupIdPrefix, 0, consumerProperties),
            topics.get(0), 0, recordHandler, followConsumers, throttledDequeuer, recoveryService);
    this.recordHandler = recordHandler;
  }

  private static Properties createConsumerProperties(String consumerGroupIdPrefix, int topicIdx, Properties baseProperties) {
    Properties properties = new Properties();
    properties.putAll(baseProperties);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupIdPrefix + topicIdx);
    return properties;
  }

  public void start() {
    logger.info("Starting {}", throttledDequeuer);
    throttledDequeuer.start();

    for(FollowConsumer<K, V> followConsumer : followConsumers) {
      logger.info("Starting {}", followConsumer);
      followConsumer.start();
    }

    logger.info("Starting {}", leadConsumer);
    leadConsumer.start();
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

  public boolean isSubscribed() {
    return leadConsumer.isSubscribed();
  }

}
