/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.assignment;

import io.thill.kafkacap.dedup.recovery.PartitionOffsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;

/**
 * Encapsulates all information required for a partition assignment event
 *
 * @param <K> The outbound topic key type
 * @param <V> The outbound topic value type
 * @author Eric Thill
 */
public class Assignment<K, V> {
  private final Map<Integer, ConsumerRecord<K, V>> lastOutboundRecords = new LinkedHashMap<>();
  private final PartitionOffsets offsets = new PartitionOffsets();
  private final Collection<Integer> partitions;
  private final int numTopics;

  /**
   * Assignment Constructor
   *
   * @param partitions The partitions that have been assigned
   * @param numTopics  The total number of inbound topics that will be listened to (totalInboundPartitions = partitions.size() * numTopics)
   */
  public Assignment(Collection<Integer> partitions, int numTopics) {
    this.partitions = partitions;
    this.numTopics = numTopics;
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
   * Set the last outbound record for a particular partition. This method must never be called by consumers of {@link Assignment}. It is meant as a utility
   * method for constructing this class.
   *
   * @param partition The partition
   * @param record    The record for this partition
   */
  public void setLastOutboundRecord(int partition, ConsumerRecord<K, V> record) {
    lastOutboundRecords.put(partition, record);
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
  public PartitionOffsets getOffsets() {
    return offsets;
  }

  /**
   * Generate a multi-line pretty-printed toString()
   *
   * @return A multi-line pretty-printed toString()
   */
  public String toPrettyString() {
    StringBuilder sb = new StringBuilder("Assignment: {");
    sb.append("\n").append(offsets.toPrettyString());
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
