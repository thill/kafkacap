/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.recovery;

import java.util.Objects;

/**
 * A key consisting of partition + topicIdx
 *
 * @author Eric Thill
 */
public class PartitionTopicIdx {

  private final int partition;
  private final int topicIdx;

  public PartitionTopicIdx(int partition, int topicIdx) {
    this.partition = partition;
    this.topicIdx = topicIdx;
  }

  public int getPartition() {
    return partition;
  }

  public int getTopicIdx() {
    return topicIdx;
  }

  @Override
  public boolean equals(Object o) {
    if(this == o)
      return true;
    if(o == null || getClass() != o.getClass())
      return false;
    PartitionTopicIdx that = (PartitionTopicIdx)o;
    return partition == that.partition &&
            topicIdx == that.topicIdx;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, topicIdx);
  }

  @Override
  public String toString() {
    return "PartitionTopicIdx{" +
            "partition=" + partition +
            ", topicIdx=" + topicIdx +
            '}';
  }
}
