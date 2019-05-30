/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.capture;

import io.thill.kafkacap.core.capture.callback.SendCompleteListener;
import io.thill.kafkacap.core.capture.populator.RecordPopulator;
import io.thill.kafkacap.core.capture.queue.CaptureQueue;
import io.thill.kafkacap.core.util.clock.Clock;
import org.agrona.concurrent.IdleStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Buffers messages using a {@link CaptureQueue} implementation then publishes them to Kafka. Instantiate using {@link BufferedPublisherBuilder}.
 *
 * @author Eric Thill
 */
public class BufferedPublisher<K, V> implements Runnable, AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final CountDownLatch closeComplete = new CountDownLatch(1);
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final AtomicBoolean flushRequired = new AtomicBoolean(false);

  private final CaptureQueue captureQueue;
  private final RecordPopulator recordPopulator;
  private final KafkaProducer<K, V> kafkaProducer;
  private final Clock clock;
  private final IdleStrategy idleStrategy;
  private final SendCompleteListener sendCompleteListener;

  private volatile CountDownLatch flushComplete;
  private volatile Thread runThread;

  BufferedPublisher(final CaptureQueue captureQueue,
                    final RecordPopulator recordPopulator,
                    final KafkaProducer<K, V> kafkaProducer,
                    final Clock clock,
                    final IdleStrategy idleStrategy,
                    final SendCompleteListener sendCompleteListener) {
    this.captureQueue = captureQueue;
    this.recordPopulator = recordPopulator;
    this.kafkaProducer = kafkaProducer;
    this.clock = clock;
    this.idleStrategy = idleStrategy;
    this.sendCompleteListener = sendCompleteListener;
  }

  /**
   * Start the {@link BufferedPublisher#run()} loop in a new thread
   */
  public void start() {
    new Thread(this, getClass().getSimpleName()).start();
  }

  @Override
  public void run() {
    try {
      runThread = Thread.currentThread();
      logger.info("Started");
      while(keepRunning.get()) {
        checkFlush();
        if(captureQueue.poll(this::send)) {
          // did work: reset idle strategy
          idleStrategy.reset();
        } else {
          // didn't do any work: don't burn CPU, user-controlled health checks
          idleStrategy.idle();
        }
      }
    } catch(Throwable t) {
      logger.error("Encountered Exception", t);
    } finally {
      logger.info("Closing...");
      logger.info("Closing {}...", captureQueue.getClass().getSimpleName());
      captureQueue.close();
      logger.info("Closing Kafka Producer...");
      kafkaProducer.close();
      logger.info("Closed");
      closeComplete.countDown();
    }
  }

  private void checkFlush() {
    if(flushRequired.get()) {
      performFlush();
      flushRequired.set(false);
      flushComplete.countDown();
    }
  }

  private void performFlush() {
    logger.debug("Flushing...");
    kafkaProducer.flush();
    logger.debug("Flush Complete");
  }

  private void send(byte[] payload, long enqueueTime) {
    // populate and send payload
    final ProducerRecord<K, V> record = recordPopulator.populate(payload, enqueueTime);
    kafkaProducer.send(record);

    // callback to SendCompleteListener: stats tracking, logging, etc
    if(sendCompleteListener != null) {
      sendCompleteListener.onSendComplete(record, enqueueTime);
    }
  }

  @Override
  public void close() throws InterruptedException {
    logger.info("Setting Close Flag...");
    keepRunning.set(false);
    logger.info("Awaiting Close Complete...");
    closeComplete.await();
    logger.info("Close Complete");
  }

  /**
   * Write the entirety of the given buffer as a single payload. The buffer reference is released immediately.
   *
   * @param buffer The payload buffer
   */
  public void write(byte[] buffer) {
    captureQueue.add(buffer, clock.now());
  }

  /**
   * Write a range of the given buffer as a single payload.  The buffer reference is released immediately.
   *
   * @param buffer The payload buffer
   * @param offset The start offset in the buffer
   * @param length The size of the payload
   */
  public void write(byte[] buffer, int offset, int length) {
    captureQueue.add(buffer, offset, length, clock.now());
  }

  /**
   * Flush and block until complete
   */
  public void flush() throws InterruptedException {
    if(Thread.currentThread() == runThread) {
      // called from the BufferedPublisher run thread, perform the flush inline
      performFlush();
    } else {
      // synchronized only with other flush calls, flag the flush and await completion
      synchronized(flushRequired) {
        flushComplete = new CountDownLatch(1);
        flushRequired.set(true);
        flushComplete.await();
        flushComplete = null;
      }
    }
  }

}
