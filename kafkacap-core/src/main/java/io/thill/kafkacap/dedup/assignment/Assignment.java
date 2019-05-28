/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.assignment;

import io.thill.kafkacap.dedup.recovery.PartitionTopicIdx;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Encapsulates all information required for a partition assignment event
 *
 * @param <K> The outbound topic key type
 * @param <V> The outbound topic value type
 * @author Eric Thill
 */
public class Assignment<K, V> {
  private final Collection<Integer> partitions;
  private final int numTopics;
  private final Map<Integer, ConsumerRecord<K, V>> lastOutboundRecords;
  private final Map<PartitionTopicIdx, Long> offsets;

  /**
   * Assignment Constructor
   *
   * @param partitions          The partitions that have been assigned
   * @param numTopics           The total number of inbound topics that will be listened to (totalInboundPartitions = partitions.size() * numTopics)
   * @param lastOutboundRecords Map of partitions to last published record
   * @param offsets             Map of partition+topicIdx to inbound offsets
   */
  public Assignment(Collection<Integer> partitions, int numTopics, Map<Integer, ConsumerRecord<K, V>> lastOutboundRecords, Map<PartitionTopicIdx, Long> offsets) {
    this.partitions = Collections.unmodifiableCollection(partitions);
    this.numTopics = numTopics;
    this.lastOutboundRecords = Collections.unmodifiableMap(lastOutboundRecords);
    this.offsets = Collections.unmodifiableMap(offsets);
  }

  /**
   * Get the assigned partitions
   *
   * @return The assigned partitions
   */
  public Collection<Integer> getPartitions() {
    return partitions;
  }

  /**
   * Get the total number of inbound capture topics
   *
   * @return The total number of inbound capture topics
   */
  public int getNumTopics() {
    return numTopics;
  }

  /**
   * Get the last outbound record for a particular partition
   *
   * @param partition The partition
   * @return The last outbound record for the given partition, or null if the outbound topic was empty
   */
  public ConsumerRecord<K, V> getLastOutboundRecord(int partition) {
    return lastOutboundRecords.get(partition);
  }

  /**
   * Get the inbound topic-partition offsets for the capture topics
   *
   * @return The inbound topic-partition offsets for the capture topics
   */
  public Map<PartitionTopicIdx, Long> getOffsets() {
    return offsets;
  }

  /**
   * Get the inbound partition offsets for the given topicIdx
   *
   * @param topicIdx The topicIdx
   * @return The inbound partition offsets for the given topicIdx
   */
  public Map<Integer, Long> getOffsetsForTopic(int topicIdx) {
    final Map<Integer, Long> result = new LinkedHashMap<>();
    for(Map.Entry<PartitionTopicIdx, Long> e : offsets.entrySet()) {
      if(e.getKey().getTopicIdx() == topicIdx) {
        result.put(e.getKey().getPartition(), e.getValue());
      }
    }
    return Collections.unmodifiableMap(result);
  }

  /**
   * Generate a multi-line pretty-printed toString()
   *
   * @return A multi-line pretty-printed toString()
   */
  public String toPrettyString() {
    StringBuilder sb = new StringBuilder("Assignment: {");
    sb.append("\nOffsets:");
    for(Map.Entry<PartitionTopicIdx, Long> e : offsets.entrySet()) {
      sb.append("\n  Partition ").append(e.getKey().getPartition()).append(" | TopicIdx ").append(e.getKey().getTopicIdx()).append(" : ").append(e.getValue());
    }
    sb.append("\nPartitions: ").append(partitions);
    sb.append("\nNumTopics: ").append(numTopics);
    sb.append("\nRecords: ");
    int numRecords = 0;
    for(Map.Entry<Integer, ConsumerRecord<K, V>> e : lastOutboundRecords.entrySet()) {
      sb.append("\n  Partition ").append(e.getKey()).append(": ").append(e.getValue().headers()); // possibly sensitive information, only display headers
      numRecords++;
    }
    if(numRecords == 0) {
      sb.append("[ none ]");
    }
    sb.append("\n}");
    return sb.toString();
  }

  @Override
  public String toString() {
    return "Assignment{" +
            "lastOutboundRecords=" + lastOutboundRecords +
            ", offsets=" + offsets +
            '}';
  }
}
