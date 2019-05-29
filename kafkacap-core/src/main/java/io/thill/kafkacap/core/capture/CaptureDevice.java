/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.capture;

import io.thill.kafkacap.core.capture.callback.AutoCleanupChronicleListener;
import io.thill.kafkacap.core.capture.callback.MultiStoreFileListener;
import io.thill.kafkacap.core.capture.callback.SendStatTracker;
import io.thill.kafkacap.core.capture.config.CaptureDeviceConfig;
import io.thill.kafkacap.core.capture.populator.RecordPopulator;
import io.thill.kafkacap.core.capture.queue.CaptureQueue;
import io.thill.kafkacap.core.capture.queue.ChronicleCaptureQueue;
import io.thill.kafkacap.core.util.clock.Clock;
import io.thill.kafkacap.core.util.clock.SystemMillisClock;
import io.thill.kafkacap.core.util.io.FileUtil;
import io.thill.trakrj.Stats;
import io.thill.trakrj.internal.tracker.ImmutableTrackerId;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Takes care of most of the ceremony of creating a typical {@link BufferedPublisher} with an underlying {@link ChronicleCaptureQueue}. An implementing class
 * simply needs to implement the {@link CaptureDevice#doWork()} method to poll from a receiver.  For more flexibility, users are also free to create their own
 * {@link BufferedPublisher} using a {@link BufferedPublisherBuilder} and write to it directly.
 *
 * @author Eric Thill
 */
public abstract class CaptureDevice<K, V> implements Runnable, AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final CountDownLatch closeComplete = new CountDownLatch(1);
  private final CaptureDeviceConfig config;
  private final IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
  private final AtomicBoolean started = new AtomicBoolean(false);
  protected final Stats stats;
  protected final BufferedPublisher<K, V> bufferedPublisher;

  /**
   * CaptureDevice Constructor
   *
   * @param config The configuration for the {@link CaptureDevice}
   * @param stats  The underlying stats instance to use
   */
  public CaptureDevice(CaptureDeviceConfig config, Stats stats) {
    this.config = config;
    this.stats = stats;
    bufferedPublisher = createBufferedPublisher(stats);
  }

  /**
   * Start the {@link CaptureDevice#run()} loop in a new thread
   */
  public void start() {
    new Thread(this, getClass().getSimpleName()).start();
  }

  @Override
  public final void run() {
    logger.info("Config: {}", config);

    try {
      logger.info("Starting BufferedPublisher...");
      bufferedPublisher.start();
      logger.info("Initializing...");
      init();
      logger.info("Initialized");
      started.set(true);

      while(keepRunning.get()) {
        if(doWork()) {
          idleStrategy.reset();
        } else {
          idleStrategy.idle();
        }
      }
    } catch(Throwable t) {
      if(keepRunning.get()) {
        logger.error("Encountered Unhandled Exception", t);
      }
    } finally {
      logger.info("Cleanup...");
      cleanup();
      logger.info("Closing Publisher...");
      tryClose(bufferedPublisher);
      logger.info("Closed");
      closeComplete.countDown();
    }
  }

  private BufferedPublisher<K, V> createBufferedPublisher(final Stats stats) {
    final Clock clock = new SystemMillisClock();

    logger.info("Deleting {}", new File(config.getChronicle().getPath()).getAbsolutePath());
    FileUtil.deleteRecursive(new File(config.getChronicle().getPath()));

    final MultiStoreFileListener storeFileListener = new MultiStoreFileListener();
    final SingleChronicleQueue chronicleQueue = SingleChronicleQueueBuilder.builder()
            .path(config.getChronicle().getPath())
            .rollCycle(config.getChronicle().getRollCycle())
            .storeFileListener(storeFileListener).build();
    final CaptureQueue captureQueue = new ChronicleCaptureQueue(chronicleQueue);

    final Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.putAll(config.getKafka().getProducer());

    final BufferedPublisher<K, V> bufferedPublisher = new BufferedPublisherBuilder<K, V>()
            .captureQueue(captureQueue)
            .recordPopulator(createRecordPopulator(config.getKafka().getTopic(), config.getKafka().getPartition(), clock))
            .clock(clock)
            .kafkaProducerProperties(kafkaProducerProperties)
            .sendCompleteListener(new SendStatTracker(clock, stats, new ImmutableTrackerId(0, "latency"), 10))
            .build();
    storeFileListener.addListener(new AutoCleanupChronicleListener(bufferedPublisher));
    return bufferedPublisher;
  }

  /**
   * Create the underlying record populator
   *
   * @param topic     The outbound Kafka topic
   * @param partition The outbound Kafka partition
   * @param clock     The clock implementation
   * @return The created {@link RecordPopulator}
   */
  protected abstract RecordPopulator<K, V> createRecordPopulator(String topic, int partition, Clock clock);

  /**
   * Starting up. Open receiver and/or resources.
   */
  protected abstract void init() throws Exception;

  /**
   * Called repeatedly to do work.  Useful for poll-based receivers. This method is allowed to block, but it is not required.
   *
   * @return True if work was done. Returning false may cause the underlying {@link IdleStrategy} to wait before trying again.
   */
  protected abstract boolean doWork() throws Exception;

  /**
   * The run/poll thread is about to exit. Close any underlying receiver and/or resources.
   */
  protected abstract void cleanup();

  /**
   * Called when close has been called. This will not be called from the run/poll thread.
   */
  protected abstract void onClose();

  private void tryClose(AutoCloseable closeable) {
    try {
      if(closeable != null) {
        closeable.close();
      }
    } catch(Throwable t) {
      logger.error("Could not close " + closeable.getClass().getSimpleName(), t);
    }
  }

  public boolean isStarted() {
    return started.get();
  }

  @Override
  public final void close() throws InterruptedException {
    onClose();
    logger.info("Setting Close Flag...");
    keepRunning.set(false);
    logger.info("Awaiting Close Complete...");
    closeComplete.await();
    logger.info("Close Complete");
  }

}
