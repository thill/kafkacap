/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.assignment;

import io.thill.kafkacap.dedup.recovery.PartitionOffsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;

public class Assignment<K, V> {
  private final Map<Integer, ConsumerRecord<K, V>> lastOutboundRecords = new LinkedHashMap<>();
  private final PartitionOffsets offsets = new PartitionOffsets();
  private final Collection<Integer> partitions;
  private final int numTopics;

  public Assignment(Collection<Integer> partitions, int numTopics) {
    this.partitions = partitions;
    this.numTopics = numTopics;
  }

  public Collection<Integer> getPartitions() {
    return partitions;
  }

  public int getNumTopics() {
    return numTopics;
  }

  public void setLastOutboundRecord(int partition, ConsumerRecord<K, V> record) {
    lastOutboundRecords.put(partition, record);
  }

  public ConsumerRecord<K, V> getLastOutboundRecord(int partition) {
    return lastOutboundRecords.get(partition);
  }

  public PartitionOffsets getOffsets() {
    return offsets;
  }

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
