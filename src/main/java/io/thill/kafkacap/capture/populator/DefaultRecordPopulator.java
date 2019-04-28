package io.thill.kafkacap.capture.populator;

import io.thill.kafkacap.clock.Clock;
import io.thill.kafkacap.constant.RecordHeaderKeys;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DefaultRecordPopulator implements RecordPopulator {

  private static final byte[] EMPTY_BUFFER = new byte[0];

  private final String topic;
  private final int partition;
  private final Clock clock;

  public DefaultRecordPopulator(String topic, int partition, Clock clock) {
    this.topic = topic;
    this.partition = partition;
    this.clock = clock;
  }

  @Override
  public ProducerRecord<byte[], byte[]> populate(byte[] payload, long enqueueTime) {
    final byte[] key = key(payload, enqueueTime);
    final byte[] value = value(payload, enqueueTime);
    final RecordHeaders headers = headers(payload, enqueueTime);
    return new ProducerRecord<>(topic, partition, key, value, headers);
  }

  protected RecordHeaders headers(byte[] payload, long enqueueTime) {
    RecordHeaders headers = new RecordHeaders();
    headers.add(RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME, longToBytes(enqueueTime));
    headers.add(RecordHeaderKeys.HEADER_KEY_CAPTURE_SEND_TIME, longToBytes(clock.now()));
    return headers;
  }

  protected byte[] key(byte[] payload, long enqueueTime) {
    return EMPTY_BUFFER;
  }

  protected byte[] value(byte[] payload, long enqueueTime) {
    return payload;
  }

  protected final byte[] longToBytes(long v) {
    return ByteBuffer.wrap(new byte[8]).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array();
  }


}
