/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.recovery;

import io.thill.kafkacap.util.io.BitUtil;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Arrays;

class TopicOffsets {

  private TopicOffset[] topicOffsets = new TopicOffset[0];

  public void offset(final int topicIdx, final long offset) {
    if(topicIdx >= topicOffsets.length)
      topicOffsets = Arrays.copyOf(topicOffsets, topicIdx+1);
    if(topicOffsets[topicIdx] == null)
      topicOffsets[topicIdx] = new TopicOffset(topicIdx);
    topicOffsets[topicIdx].setOffset(offset);
  }

  public int size() {
    int size = 0;
    for(int i = 0; i < topicOffsets.length; i++) {
      if(topicOffsets[i] != null) {
        size++;
      }
    }
    return size;
  }

  public void populateHeaders(RecordHeaders headers) {
    for(int i = 0; i < topicOffsets.length; i++) {
      if(topicOffsets[i] != null) {
        headers.add(topicOffsets[i].getHeaderKey(), BitUtil.longToBytes(topicOffsets[i].getOffset()));
      }
    }
  }

  public void forEach(final TopicOffsetFunction func) {
    for(int i = 0; i < topicOffsets.length; i++) {
      if(topicOffsets[i] != null) {
        func.offset(topicOffsets[i].getTopicIdx(), topicOffsets[i].getOffset());
      }
    }
  }

  public interface TopicOffsetFunction {
    void offset(int topicIdx, long offset);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("TopicOffsets{");
    for(int i = 0; i < topicOffsets.length; i++) {
      if(topicOffsets[i] != null) {
        sb.append(topicOffsets[i]).append(" ");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  // testing
  Long getOffset(final int topicIdx) {
    if(topicOffsets[topicIdx] != null) {
      return topicOffsets[topicIdx].getOffset();
    }
    return null;
  }
}
