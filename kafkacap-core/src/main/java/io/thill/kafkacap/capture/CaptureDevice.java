package io.thill.kafkacap.capture;

import io.thill.kafkacap.capture.callback.AutoCleanupChronicleListener;
import io.thill.kafkacap.capture.callback.MultiStoreFileListener;
import io.thill.kafkacap.capture.callback.SendStatTracker;
import io.thill.kafkacap.capture.config.CaptureDeviceConfig;
import io.thill.kafkacap.capture.populator.DefaultRecordPopulator;
import io.thill.kafkacap.capture.queue.CaptureQueue;
import io.thill.kafkacap.capture.queue.ChronicleCaptureQueue;
import io.thill.kafkacap.util.clock.Clock;
import io.thill.kafkacap.util.clock.SystemMillisClock;
import io.thill.kafkacap.util.io.FileUtil;
import io.thill.trakrj.Stats;
import io.thill.trakrj.internal.tracker.ImmutableTrackerId;
import io.thill.trakrj.logger.Slf4jStatLogger;
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
 * simply needs to implement to {@link CaptureDevice#poll(BufferHandler)} method to poll from a receiver.  For more flexibility, users are also free to create
 * their own {@link BufferedPublisher} using a {@link BufferedPublisherBuilder} and write to it directly.
 *
 * @author Eric Thill
 */
public abstract class CaptureDevice implements Runnable, AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final CountDownLatch closeComplete = new CountDownLatch(1);
  private final CaptureDeviceConfig config;
  private final IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

  public CaptureDevice(CaptureDeviceConfig config) {
    this.config = config;
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

    logger.info("Deleting {}", new File(config.getChronicle().getPath()).getAbsolutePath());
    FileUtil.deleteRecursive(new File(config.getChronicle().getPath()));

    logger.info("Starting Stats...");
    final Stats stats = Stats.create(new Slf4jStatLogger());

    logger.info("Creating BufferedPublisher...");
    final BufferedPublisher<byte[], byte[]> bufferedPublisher = createBufferedPublisher(stats);

    try {
      logger.info("Starting BufferedPublisher...");
      bufferedPublisher.start();
      logger.info("Initializing...");
      init();
      logger.info("Initialized");

      while(keepRunning.get()) {
        if(poll(bufferedPublisher::write)) {
          idleStrategy.reset();
        } else {
          idleStrategy.idle();
        }
      }
    } catch(Throwable t) {
      logger.error("Encountered Unhandled Exception", t);
    } finally {
      logger.info("Closing Stats...");
      stats.close();
      logger.info("Cleanup...");
      cleanup();
      logger.info("Closing Publisher...");
      tryClose(bufferedPublisher);
      logger.info("Closed");
      closeComplete.countDown();
    }
  }

  private BufferedPublisher<byte[], byte[]> createBufferedPublisher(final Stats stats) {
    final Clock clock = new SystemMillisClock();
    final MultiStoreFileListener storeFileListener = new MultiStoreFileListener();
    final SingleChronicleQueue chronicleQueue = SingleChronicleQueueBuilder.builder()
            .path(config.getChronicle().getPath())
            .rollCycle(config.getChronicle().getRollCycle())
            .storeFileListener(storeFileListener).build();
    final CaptureQueue captureQueue = new ChronicleCaptureQueue(chronicleQueue);
    final Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.putAll(config.getKafka().getProducer());
    final BufferedPublisher<byte[], byte[]> bufferedPublisher = new BufferedPublisherBuilder<byte[], byte[]>()
            .captureQueue(captureQueue)
            .recordPopulator(new DefaultRecordPopulator<>(config.getKafka().getTopic(), config.getKafka().getPartition(), clock))
            .clock(clock)
            .kafkaProducerProperties(kafkaProducerProperties)
            .sendCompleteListener(new SendStatTracker(clock, stats, new ImmutableTrackerId(0, "latency"), 10))
            .build();
    storeFileListener.addListener(new AutoCleanupChronicleListener(bufferedPublisher));
    return bufferedPublisher;
  }

  /**
   * Poll the next payload. This method may or may not block.
   *
   * @param handler The buffer handler
   * @return True if a message was received
   */
  protected abstract boolean poll(BufferHandler handler) throws Exception;

  /**
   * Starting up. Open receiver and/or resources.
   */
  protected abstract void init() throws Exception;

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

  @Override
  public final void close() throws InterruptedException {
    onClose();
    logger.info("Setting Close Flag...");
    keepRunning.set(false);
    logger.info("Awaiting Close Complete...");
    closeComplete.await();
    logger.info("Close Complete");
  }

  public interface BufferHandler {
    void handle(byte[] buffer, int offset, int length);
  }
}