package io.thill.kafkacap.capture;

import io.thill.kafkacap.capture.callback.SendCompleteListener;
import io.thill.kafkacap.capture.populator.RecordPopulator;
import io.thill.kafkacap.util.clock.Clock;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.agrona.concurrent.IdleStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Buffers messages to disk using <a href="https://github.com/OpenHFT/Chronicle-Queue">Chronicle-Queue</a> then publishes them to Kafka
 *
 * @author Eric Thill
 */
public class BufferedPublisher<K, V> implements Runnable, AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final CountDownLatch closeComplete = new CountDownLatch(1);
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final AtomicBoolean flushRequired = new AtomicBoolean(false);

  private final SingleChronicleQueue chronicleQueue;
  private final ExcerptAppender chronicleAppender;
  private final ExcerptTailer chronicleTailer;
  private final RecordPopulator recordPopulator;
  private final KafkaProducer<K, V> kafkaProducer;
  private final Clock clock;
  private final IdleStrategy idleStrategy;
  private final SendCompleteListener sendCompleteListener;

  private volatile CountDownLatch flushComplete;

  BufferedPublisher(final SingleChronicleQueue chronicleQueue,
                    final RecordPopulator recordPopulator,
                    final KafkaProducer<K, V> kafkaProducer,
                    final Clock clock,
                    final IdleStrategy idleStrategy,
                    final SendCompleteListener sendCompleteListener) {
    this.chronicleQueue = chronicleQueue;
    this.chronicleAppender = chronicleQueue.acquireAppender();
    this.chronicleTailer = chronicleQueue.createTailer().toEnd();
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
      logger.info("Started");
      while(keepRunning.get()) {
        checkFlush();
        if(readFromChronicle()) {
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
      logger.info("Closing Chronicle Queue...");
      chronicleQueue.close();
      logger.info("Closing Kafka Producer...");
      kafkaProducer.close();
      logger.info("Closed");
      closeComplete.countDown();
    }
  }

  private void checkFlush() {
    if(flushRequired.get()) {
      logger.info("Flushing...");
      kafkaProducer.flush();
      logger.info("Flush Complete");
      flushRequired.set(false);
      flushComplete.countDown();
    }
  }

  private boolean readFromChronicle() {
    return chronicleTailer.readBytes(b -> {
      // parse payload
      final int length = b.readInt();
      final long enqueueTime = b.readLong();
      final byte[] payload = new byte[length];
      b.read(payload);

      // populate and send payload
      final ProducerRecord<K, V> record = recordPopulator.populate(payload, enqueueTime);
      kafkaProducer.send(record);

      // callback to SendCompleteListener: stats tracking, logging, etc
      if(sendCompleteListener != null) {
        sendCompleteListener.onSendComplete(record, enqueueTime);
      }
    });
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
   * Write the entirety of the given buffer as a single payload
   *
   * @param buffer The payload buffer
   */
  public void write(byte[] buffer) {
    write(buffer, 0, buffer.length);
  }

  /**
   * Write a range of the given buffer as a single payload
   *
   * @param buffer The payload buffer
   * @param offset The start offset in the buffer
   * @param length The size of the payload
   */
  public void write(byte[] buffer, int offset, int length) {
    chronicleAppender.writeBytes(b -> {
      b.writeInt(length);
      b.writeLong(clock.now());
      b.write(buffer, offset, length);
    });
  }

  /**
   * Flush and block until complete
   */
  public synchronized void flush() {
    // synchronized only with other flush calls
    flushComplete = new CountDownLatch(1);
    flushRequired.set(true);
    flushComplete.countDown();
    flushComplete = null;
  }

}
