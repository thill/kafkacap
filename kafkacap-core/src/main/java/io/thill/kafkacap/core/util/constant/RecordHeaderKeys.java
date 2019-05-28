/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.util.constant;

/**
 * Common keys for Kafka {@link org.apache.kafka.common.header.Headers}
 */
public class RecordHeaderKeys {

  public static final String HEADER_KEY_CAPTURE_QUEUE_TIME = "CAPQT";
  public static final String HEADER_KEY_CAPTURE_SEND_TIME = "CAPST";
  public static final String HEADER_KEY_DEDUP_SEND_TIME = "DDST";
  public static final String HEADER_KEY_DEDUP_OFFSET_PREFIX = "DDOFF_";
  public static final String HEADER_KEY_DEDUP_SEQUENCE = "DDSEQ";
  public static final String HEADER_KEY_DEDUP_NUM_SEQUENCES = "DDNUM";

}
