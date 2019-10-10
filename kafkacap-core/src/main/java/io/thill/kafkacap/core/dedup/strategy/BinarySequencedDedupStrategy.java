package io.thill.kafkacap.core.dedup.strategy;

import org.agrona.BufferUtil;
import org.agrona.UnsafeAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.Map;

/**
 * A {@link SequencedDedupStrategy} that parses a binary 64-bit integer from the payload.
 *
 * @param <K> kafka record key type
 * @param <V> kafka record value type
 * @author Eric Thill
 */
public abstract class BinarySequencedDedupStrategy<K, V> extends SequencedDedupStrategy<K, V> {

  private static final String KEY_OFFSET = "offset";
  private static final String KEY_BYTE_ORDER = "byteOrder";
  private static final String KEY_ORDERED_CAPTURE = "orderedCapture";
  private static final String DEFAULT_ORDERED_CAPTURE = "false";
  private static final String KEY_SEQUENCE_GAP_MILLIS_TIMEOUT = "sequenceGapTimeoutMillis";
  private static final String DEFAULT_SEQUENCE_GAP_MILLIS_TIMEOUT = "10000";
  private static final String KEY_HEADER_PREFIX = "headerPrefix";
  private static final String DEFAULT_HEADER_PREFIX = "";

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final int offset;
  private final ByteOrder byteOrder;

  /**
   * Configuration parsing Constructor
   *
   * @param props The configured properties
   *              <br>offset: the offset of the sequence in the binary message
   *              <br>byteOrder: the byte order of the sequence number
   *              <br>orderedCapture: flag if the inbound stream is guaranteed to be ordered
   *              <br>sequenceGapTimeoutMillis: number of milliseconds to wait for a sequence before logging an error and skipping it
   *              <br>headerPrefix: prefix for all headers to be added
   */
  public BinarySequencedDedupStrategy(Map<String, String> props) {
    this(
            Boolean.parseBoolean(props.getOrDefault(KEY_ORDERED_CAPTURE, DEFAULT_ORDERED_CAPTURE)),
            Long.parseLong(props.getOrDefault(KEY_SEQUENCE_GAP_MILLIS_TIMEOUT, DEFAULT_SEQUENCE_GAP_MILLIS_TIMEOUT)),
            props.getOrDefault(KEY_HEADER_PREFIX, DEFAULT_HEADER_PREFIX),
            Integer.parseInt(props.get(KEY_OFFSET)),
            parseByteOrder(props.get(KEY_BYTE_ORDER))
    );
  }

  private static ByteOrder parseByteOrder(String str) {
    if(ByteOrder.LITTLE_ENDIAN.toString().equals(str))
      return ByteOrder.LITTLE_ENDIAN;
    else if(ByteOrder.BIG_ENDIAN.toString().equals(str))
      return ByteOrder.BIG_ENDIAN;
    else
      throw new IllegalArgumentException("Invalid ByteOrder: " + str);
  }

  /**
   * Constructor
   *
   * @param orderedCapture           flag if the inbound stream is guaranteed to be ordered
   * @param sequenceGapMillisTimeout number of milliseconds to wait for a sequence before logging an error and skipping it
   * @param headerPrefix             prefix for all headers to be added
   * @param offset                   the offset of the sequence in the binary message
   * @param byteOrder                the byte order of the sequence number
   */
  public BinarySequencedDedupStrategy(boolean orderedCapture, long sequenceGapMillisTimeout, String headerPrefix, Integer offset, ByteOrder byteOrder) {
    super(orderedCapture, sequenceGapMillisTimeout, headerPrefix);
    if(offset == null)
      throw new IllegalArgumentException(KEY_OFFSET + " cannot be null");
    if(byteOrder == null)
      throw new IllegalArgumentException(KEY_BYTE_ORDER + " cannot be null");
    this.offset = offset;
    this.byteOrder = byteOrder;
  }

  @Override
  protected void onSequenceGap(int partition, long fromSequence, long toSequence) {
    logger.error("Sequence Gap Detected. partition={} fromSequence={} toSequence={}", partition, fromSequence, toSequence);
  }

  protected long parseSequence(byte[] bytes) {
    if(offset + 8 > bytes.length)
      throw new ArrayIndexOutOfBoundsException(offset + 8);
    long bits = UnsafeAccess.UNSAFE.getLong(bytes, BufferUtil.ARRAY_BASE_OFFSET + (long)offset);
    if(BufferUtil.NATIVE_BYTE_ORDER != byteOrder) {
      bits = Long.reverseBytes(bits);
    }
    return bits;
  }
}
