package io.thill.kafkacap.core.dedup.strategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.ByteOrder;
import java.util.Map;

/**
 * A @{@link BinarySequencedDedupStrategy} that reads from Kafka {@link ConsumerRecord#key()}
 *
 * @author Eric Thill
 */
public class BinarySequencedKeyDedupStrategy extends BinarySequencedDedupStrategy<byte[], Object> {

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
  public BinarySequencedKeyDedupStrategy(Map<String, String> props) {
    super(props);
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
  public BinarySequencedKeyDedupStrategy(boolean orderedCapture, long sequenceGapMillisTimeout, String headerPrefix, Integer offset, ByteOrder byteOrder) {
    super(orderedCapture, sequenceGapMillisTimeout, headerPrefix, offset, byteOrder);
  }

  @Override
  protected long parseSequence(ConsumerRecord<byte[], Object> record) {
    return parseSequence(record.key());
  }

}
