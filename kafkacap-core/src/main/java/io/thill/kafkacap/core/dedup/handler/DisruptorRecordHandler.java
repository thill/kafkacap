/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.handler;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.thill.kafkacap.core.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

/**
 * An implementation of a {@link RecordHandler} that uses a {@link Disruptor} for concurrency, queueing, and threading.  See the <a
 * href="https://github.com/LMAX-Exchange/disruptor">Disruptor project on github</a> for more information
 *
 * @param <K> The kafka record key type
 * @param <V> The kafka record value type
 * @author Eric Thill
 */
public class DisruptorRecordHandler<K, V> implements RecordHandler<K, V> {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Disruptor<RecordEvent> disruptor;
  private final RingBuffer<RecordEvent> ringBuffer;
  private final RecordHandler<K, V> underlyingRecordHandler;

  /**
   * DisruptorRecordHandler Constructor
   *
   * @param underlyingRecordHandler The underlying {@link RecordHandler} which will have all calls made from a single thread.
   * @param ringBufferSize          The {@link Disruptor}'s {@link RingBuffer} size
   * @param waitStrategy            The {@link Disruptor}'s {@link WaitStrategy}
   */
  public DisruptorRecordHandler(RecordHandler<K, V> underlyingRecordHandler, int ringBufferSize, WaitStrategy waitStrategy) {
    this.underlyingRecordHandler = underlyingRecordHandler;
    this.disruptor = new Disruptor<>(RecordEvent::new, ringBufferSize, Executors.defaultThreadFactory(), ProducerType.MULTI, waitStrategy);
    this.disruptor.handleEventsWith(recordEventHandler);
    this.ringBuffer = disruptor.getRingBuffer();
  }

  @Override
  public void start() {
    logger.info("Starting Disruptor...");
    disruptor.start();
    logger.info("Disruptor Started");

    dispatch(RecordEventType.START, null, -1, -1, null, true);
  }

  @Override
  public void close() {
    dispatch(RecordEventType.CLOSE, null, -1, -1, null, true);

    logger.info("Shutting Down Disruptor...");
    disruptor.shutdown();
    logger.info("Disruptor Shutdown Complete");
  }

  @Override
  public void handle(final ConsumerRecord<K, V> record, final int topicIdx) {
    dispatch(RecordEventType.HANDLE, record, topicIdx, -1, null, false);
  }

  @Override
  public void flush() {
    dispatch(RecordEventType.FLUSH, null, -1, -1, null, true);
  }

  @Override
  public void checkCache(final int partition) {
    dispatch(RecordEventType.CHECK_CACHE, null, -1, partition, null, false);
  }

  @Override
  public void assigned(final Assignment<K, V> assignment) {
    dispatch(RecordEventType.ASSIGNED, null, -1, -1, assignment, true);
  }

  @Override
  public void revoked() {
    dispatch(RecordEventType.REVOKED, null, -1, -1, null, true);
  }

  private void dispatch(final RecordEventType type,
                        final ConsumerRecord<K, V> record,
                        final int topicIdx,
                        final int partition,
                        final Assignment<K, V> assignment,
                        final boolean block) {
    final CountDownLatch completeLatch = block ? new CountDownLatch(1) : null;

    final long seq = ringBuffer.next();
    final RecordEvent event = ringBuffer.get(seq);
    event.type = type;
    event.record = record;
    event.topicIdx = topicIdx;
    event.partition = partition;
    event.assignment = assignment;
    event.completeLatch = completeLatch;
    ringBuffer.publish(seq);

    if(block) {
      logger.debug("Dispatched {} Event - Awaiting Completion...", type);
      try {
        completeLatch.await();
      } catch(InterruptedException e) {
        logger.error("Interrupted waiting for revoke to complete", e);
      }
      logger.debug("{} Event Complete", type);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  private final EventHandler<RecordEvent> recordEventHandler = new EventHandler<RecordEvent>() {
    @Override
    public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
      try {
        switch(event.type) {
          case HANDLE:
            underlyingRecordHandler.handle(event.record, event.topicIdx);
            break;
          case FLUSH:
            underlyingRecordHandler.flush();
            break;
          case CHECK_CACHE:
            underlyingRecordHandler.checkCache(event.partition);
            break;
          case ASSIGNED:
            underlyingRecordHandler.assigned(event.assignment);
            break;
          case REVOKED:
            underlyingRecordHandler.revoked();
            break;
          case START:
            logger.info("Starting underlying RecordHandler");
            underlyingRecordHandler.start();
            logger.info("Started");
            break;
          case CLOSE:
            logger.info("Closing underlying RecordHandler");
            underlyingRecordHandler.close();
            logger.info("Closed");
            break;
        }
      } catch(Throwable t) {
        logger.error("Error handling: " + event, t);
      } finally {
        if(event.completeLatch != null) {
          event.completeLatch.countDown();
        }
        event.reset();
      }
    }
  };

  private class RecordEvent {
    private RecordEventType type;
    private ConsumerRecord<K, V> record;
    private int topicIdx;
    private int partition;
    private Assignment<K, V> assignment;
    private CountDownLatch completeLatch;

    public RecordEvent() {
      reset();
    }

    public void reset() {
      type = null;
      record = null;
      topicIdx = -1;
      partition = -1;
      assignment = null;
      completeLatch = null;
    }

    @Override
    public String toString() {
      return "RecordEvent{" +
              "type=" + type +
              ", record=" + record +
              ", topicIdx=" + topicIdx +
              ", partition=" + partition +
              ", assignment=" + assignment +
              '}';
    }
  }

  private enum RecordEventType {
    START, CLOSE, HANDLE, CHECK_CACHE, ASSIGNED, REVOKED, FLUSH
  }
}
