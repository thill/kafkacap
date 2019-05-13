/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.recovery;

import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Keeps track of the last inbound offset per topicIdx+partition. Topic indexes are used instead of topic names for efficiency.
 *
 * @author Eric Thill
 */
public class PartitionOffsets {
  private TopicOffsets[] offsetsPerPartition = new TopicOffsets[0];

  /**
   * Report a new offset for a given partition+topicIdx
   *
   * @param partition The partition
   * @param topicIdx  The topicIdx
   * @param offset    The offset
   */
  public void offset(final int partition, final int topicIdx, final long offset) {
    if(partition >= offsetsPerPartition.length)
      offsetsPerPartition = Arrays.copyOf(offsetsPerPartition, partition + 1);
    if(offsetsPerPartition[partition] == null)
      offsetsPerPartition[partition] = new TopicOffsets();
    offsetsPerPartition[partition].offset(topicIdx, offset);
  }

  /**
   * Get the number of offsets currently being tracked
   *
   * @return The number of offsets currently being tracked
   */
  public int size() {
    int size = 0;
    for(int i = 0; i < offsetsPerPartition.length; i++) {
      if(offsetsPerPartition[i] != null) {
        size += offsetsPerPartition[i].size();
      }
    }
    return size;
  }

  /**
   * Get a list of current partition offsets for a given topicIdx
   *
   * @param topicIdx The topicIdx
   * @return Partition:Offset map for the given topicIdx
   */
  public Map<Integer, Long> topicOffsets(int topicIdx) {
    Map<Integer, Long> results = new LinkedHashMap<>();
    forEach((int partition, int otherTopicIdx, long offset) -> {
      if(otherTopicIdx == topicIdx) {
        results.put(partition, offset);
      }
    });
    return results;
  }

  /**
   * Use the internal state to populate record headers
   *
   * @param headers The headers to populate
   */
  public void populateHeaders(RecordHeaders headers) {
    for(int i = 0; i < offsetsPerPartition.length; i++) {
      if(offsetsPerPartition[i] != null) {
        offsetsPerPartition[i].populateHeaders(headers);
      }
    }
  }

  /**
   * Iterate over all topicIdx+partition offset entries
   *
   * @param func Function to handle each entry
   */
  public void forEach(final PartitionOffsetFunction func) {
    for(int i = 0; i < offsetsPerPartition.length; i++) {
      final int partition = i;
      if(offsetsPerPartition[i] != null) {
        offsetsPerPartition[i].forEach(((topicIdx, offset) -> func.offset(partition, topicIdx, offset)));
      }
    }
  }

  public interface PartitionOffsetFunction {
    void offset(int partition, int topicIdx, long offset);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PartitionOffsets{");
    forEach((int partition, int topicIdx, long offset) ->
            sb.append("[partition=").append(partition).append(" topicIdx=").append(topicIdx).append(" offset=").append(offset).append("] ")
    );
    return sb.append('}').toString();
  }

  /**
   * Create a pretty-printed multi-line toString()
   *
   * @return Create a pretty-printed multi-line toString()
   */
  public String toPrettyString() {
    final StringBuilder sb = new StringBuilder("PartitionOffsets: ");
    int totalPartitions = 0;
    for(int partition = 0; partition < offsetsPerPartition.length; partition++) {
      if(offsetsPerPartition[partition] != null) {
        sb.append("\n  Partition ").append(partition).append(":");
        offsetsPerPartition[partition].forEach((topicIdx, offset) -> {
          sb.append("\n    TopicIdx ").append(topicIdx).append(": ").append(offset);
        });
        totalPartitions++;
      }
    }
    if(totalPartitions == 0) {
      sb.append("[ none ]");
    }
    return sb.toString();
  }

  // testing
  Long getOffset(final int partition, final int topicIdx) {
    if(offsetsPerPartition[partition] != null) {
      return offsetsPerPartition[partition].getOffset(topicIdx);
    }
    return null;
  }
}
