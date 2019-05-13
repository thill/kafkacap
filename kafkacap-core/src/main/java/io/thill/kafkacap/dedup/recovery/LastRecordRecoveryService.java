/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.recovery;

import io.thill.kafkacap.dedup.assignment.Assignment;
import io.thill.kafkacap.util.constant.RecordHeaderKeys;
import io.thill.kafkacap.util.io.BitUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * A {@link RecoveryService} implementation that is populated by polling the last record from the outbound topic
 *
 * @param <K> The record key type
 * @param <V> The record value type
 * @author Eric Thill
 */
public class LastRecordRecoveryService<K, V> implements RecoveryService<K, V> {

  private static final Duration POLL_DURATION = Duration.ofSeconds(10);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Properties consumerProperties = new Properties();
  private final String outboundTopic;
  private final int numInboundTopics;

  /**
   * LastRecordRecoveryService constructor
   *
   * @param consumerProperties The properties used to instantiate the {@link KafkaConsumer}
   * @param outboundTopic      The outbound topic to poll records from
   * @param numInboundTopics   The total number of inbound topics
   */
  public LastRecordRecoveryService(Properties consumerProperties, String outboundTopic, int numInboundTopics) {
    this.consumerProperties.putAll(consumerProperties);
    this.consumerProperties.remove(ConsumerConfig.GROUP_ID_CONFIG);
    this.outboundTopic = outboundTopic;
    this.numInboundTopics = numInboundTopics;
  }

  /**
   * Polls the last inbound offsets from the headers of the last published outbound message
   *
   * @return the populated PartitionOffsets. If no record exists, PartitionOffsets will be instantiated, but empty.
   */
  @Override
  public Assignment<K, V> recover(Collection<Integer> partitions) {
    logger.info("Polling last records for partitions {}", partitions);
    final Assignment assignment = new Assignment(partitions, numInboundTopics);

    for(final int partition : partitions) {
      final TopicPartition topicPartition = new TopicPartition(outboundTopic, partition);
      logger.info("Polling last record from {}", topicPartition);
      try(KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
        consumer.assign(Arrays.asList(topicPartition));
        ConsumerRecords<?, ?> records = ConsumerRecords.empty();
        while(records.isEmpty()) {
          consumer.seekToBeginning(Arrays.asList(topicPartition));
          final long startPosition = consumer.position(topicPartition);
          logger.info("Start position for {} is {}", topicPartition, startPosition);
          consumer.seekToEnd(Arrays.asList(topicPartition));
          final long endPosition = consumer.position(topicPartition);
          logger.info("End position for {} is {}", topicPartition, endPosition);

          if(endPosition <= startPosition) {
            logger.info("No record to recover from {}", topicPartition);
            break;
          }

          final long lastRecordPosition = endPosition - 1;
          consumer.seek(topicPartition, lastRecordPosition);
          logger.info("Polling last record from position {}", lastRecordPosition);
          records = consumer.poll(POLL_DURATION);
        }

        if(!records.isEmpty()) {
          final ConsumerRecord<?, ?> record = records.iterator().next();
          assignment.setLastOutboundRecord(record.partition(), record);
          logger.info("Parsing offsets from {}", record.headers());
          for(Header header : record.headers()) {
            if(header.key().startsWith(RecordHeaderKeys.HEADER_KEY_DEDUP_OFFSET_PREFIX)) {
              final int topicIdx = Integer.parseInt(header.key().substring(RecordHeaderKeys.HEADER_KEY_DEDUP_OFFSET_PREFIX.length()));
              final long offset = BitUtil.bytesToLong(header.value());
              logger.info("Recovered: partition={} topicIdx={} offset={}", partition, topicIdx, offset);
              assignment.getOffsets().offset(partition, topicIdx, offset);
            }
          }
        }
      }
    }

    logger.info("Recovered {}", assignment.toPrettyString());
    return assignment;
  }

}
