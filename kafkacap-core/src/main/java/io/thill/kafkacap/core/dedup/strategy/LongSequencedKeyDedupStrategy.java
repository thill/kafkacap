package io.thill.kafkacap.core.dedup.strategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.Map;

/**
 * A @{@link SequencedDedupStrategy} that uses a {@link Long} {@link ConsumerRecord#key()} as the sequence
 *
 * @author Eric Thill
 */
public class LongSequencedKeyDedupStrategy extends SequencedDedupStrategy<Long, Object> {

  private static final String KEY_ORDERED_CAPTURE = "orderedCapture";
  private static final String DEFAULT_ORDERED_CAPTURE = "false";
  private static final String KEY_SEQUENCE_GAP_MILLIS_TIMEOUT = "sequenceGapTimeoutMillis";
  private static final String DEFAULT_SEQUENCE_GAP_MILLIS_TIMEOUT = "10000";
  private static final String KEY_HEADER_PREFIX = "headerPrefix";
  private static final String DEFAULT_HEADER_PREFIX = "";

  /**
   * Configuration parsing Constructor
   *
   * @param props The configured properties
   *              <br>orderedCapture: flag if the inbound stream is guaranteed to be ordered
   *              <br>sequenceGapTimeoutMillis: number of milliseconds to wait for a sequence before logging an error and skipping it
   *              <br>headerPrefix: prefix for all headers to be added
   */
  public LongSequencedKeyDedupStrategy(Map<String, String> props) {
    super(
            Boolean.parseBoolean(props.getOrDefault(KEY_ORDERED_CAPTURE, DEFAULT_ORDERED_CAPTURE)),
            Long.parseLong(props.getOrDefault(KEY_SEQUENCE_GAP_MILLIS_TIMEOUT, DEFAULT_SEQUENCE_GAP_MILLIS_TIMEOUT)),
            props.getOrDefault(KEY_HEADER_PREFIX, DEFAULT_HEADER_PREFIX)
    );
  }

  /**
   * Constructor
   *
   * @param orderedCapture           flag if the inbound stream is guaranteed to be ordered
   * @param sequenceGapMillisTimeout number of milliseconds to wait for a sequence before logging an error and skipping it
   * @param headerPrefix             prefix for all headers to be added
   */
  public LongSequencedKeyDedupStrategy(boolean orderedCapture, long sequenceGapMillisTimeout, String headerPrefix) {
    super(orderedCapture, sequenceGapMillisTimeout, headerPrefix);
  }

  @Override
  protected long parseSequence(ConsumerRecord<Long, Object> record) {
    return record.key();
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  protected void onSequenceGap(int partition, long fromSequence, long toSequence) {
    logger.error("Sequence Gap Detected. partition={} fromSequence={} toSequence={}", partition, fromSequence, toSequence);
  }
}
