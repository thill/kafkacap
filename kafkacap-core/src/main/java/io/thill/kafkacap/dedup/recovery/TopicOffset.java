/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.recovery;

import io.thill.kafkacap.util.constant.RecordHeaderKeys;

class TopicOffset {
  private final int topicIdx;
  private final String headerKey;
  private long offset;

  public TopicOffset(int topicIdx) {
    this.topicIdx = topicIdx;
    headerKey = RecordHeaderKeys.HEADER_KEY_DEDUP_OFFSET_PREFIX + topicIdx;
  }

  public int getTopicIdx() {
    return topicIdx;
  }

  public String getHeaderKey() {
    return headerKey;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public String toString() {
    return "TopicOffset{" +
            "topicIdx=" + topicIdx +
            ", headerKey='" + headerKey + '\'' +
            ", offset=" + offset +
            '}';
  }
}
