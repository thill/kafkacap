package io.thill.kafkacap.capture;

import io.thill.kafkacap.capture.callback.AutoCleanupChronicleListener;
import io.thill.kafkacap.capture.callback.MultiStoreFileListener;
import io.thill.kafkacap.capture.callback.SendCompleteListener;
import io.thill.kafkacap.capture.config.PublisherConfig;
import io.thill.kafkacap.capture.populator.DefaultRecordPopulator;
import io.thill.kafkacap.capture.populator.RecordPopulator;
import io.thill.kafkacap.util.clock.Clock;
import io.thill.kafkacap.util.clock.SystemMillisClock;
import io.thill.kafkacap.util.io.FileUtil;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class BufferedPublisherBuilder<K, V> {

  private String chronicleQueuePath;
  private RollCycle chronicleQueueRollCycle = RollCycles.MINUTELY;
  private StoreFileListener chronicleStoreFileListener;
  private boolean chronicleQueueCleanOnRoll;
  private boolean chronicleQueueCleanOnStartup;
  private SingleChronicleQueue chronicleQueue;
  private RecordPopulator<K, V> recordPopulator;
  private KafkaProducer<K, V> kafkaProducer;
  private Properties kafkaProducerProperties;
  private Clock clock = new SystemMillisClock();
  private IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
  private SendCompleteListener sendCompleteListener;

  /**
   * Set all properties from a config
   *
   * @param config
   * @return
   */
  public BufferedPublisherBuilder<K, V> config(PublisherConfig config) {
    if(config.getChronicle() != null) {
      if(config.getChronicle().getPath() != null)
        chronicleQueuePath(config.getChronicle().getPath());
      if(config.getChronicle().getRollCycle() != null)
        chronicleQueueRollCycle(config.getChronicle().getRollCycle());
    }
    if(config.getKafka() != null) {
      if(config.getKafka().getProducer() != null)
        kafkaProducerProperties(config.getKafka().getProducer());
      if(config.getKafka().getTopic() != null && config.getKafka().getPartition() != null)
        recordPopulator(new DefaultRecordPopulator(config.getKafka().getTopic(), config.getKafka().getPartition(), clock));
      if(config.getChronicle().isCleanOnRoll())
        chronicleQueueCleanOnRoll(config.getChronicle().isCleanOnRoll());
      if(config.getChronicle().isCleanOnStartup())
        chronicleQueueCleanOnStartup(config.getChronicle().isCleanOnStartup());
    }
    return this;
  }

  /**
   * Used to create the {@link SingleChronicleQueue} when {@link BufferedPublisherBuilder#chronicleQueue(SingleChronicleQueue)} is unset
   *
   * @param chronicleQueuePath
   * @return
   */
  public BufferedPublisherBuilder<K, V> chronicleQueuePath(String chronicleQueuePath) {
    this.chronicleQueuePath = chronicleQueuePath;
    return this;
  }

  /**
   * Used to create the {@link SingleChronicleQueue} when {@link BufferedPublisherBuilder#chronicleQueue(SingleChronicleQueue)} is unset
   *
   * @param chronicleQueueRollCycle
   * @return
   */
  public BufferedPublisherBuilder<K, V> chronicleQueueRollCycle(RollCycle chronicleQueueRollCycle) {
    this.chronicleQueueRollCycle = chronicleQueueRollCycle;
    return this;
  }

  /**
   * Optional. Used to create the {@link SingleChronicleQueue} when {@link BufferedPublisherBuilder#chronicleQueue(SingleChronicleQueue)} is unset
   *
   * @param chronicleStoreFileListener
   * @return
   */
  public BufferedPublisherBuilder<K, V> chronicleStoreFileListener(StoreFileListener chronicleStoreFileListener) {
    this.chronicleStoreFileListener = chronicleStoreFileListener;
    return this;
  }

  /**
   * Flag to create an {@link AutoCleanupChronicleListener} for the Chronicle Queue when {@link BufferedPublisherBuilder#chronicleQueue(SingleChronicleQueue)}
   * is unset
   *
   * @param chronicleQueueCleanOnRoll
   * @return
   */
  public BufferedPublisherBuilder<K, V> chronicleQueueCleanOnRoll(boolean chronicleQueueCleanOnRoll) {
    this.chronicleQueueCleanOnRoll = chronicleQueueCleanOnRoll;
    return this;
  }

  /**
   * Flag to delete the directory at {@link BufferedPublisherBuilder#chronicleQueuePath(String)} on startup
   *
   * @param chronicleQueueCleanOnStartup
   * @return
   */
  public BufferedPublisherBuilder<K, V> chronicleQueueCleanOnStartup(boolean chronicleQueueCleanOnStartup) {
    this.chronicleQueueCleanOnStartup = chronicleQueueCleanOnStartup;
    return this;
  }

  /**
   * The {@link SingleChronicleQueue} to use internally. When unset, it will be created automatically using {@link
   * BufferedPublisherBuilder#chronicleQueuePath(String)}
   *
   * @param chronicleQueue
   * @return
   */
  public BufferedPublisherBuilder<K, V> chronicleQueue(SingleChronicleQueue chronicleQueue) {
    this.chronicleQueue = chronicleQueue;
    return this;
  }

  /**
   * Used to populate the outbound {@link org.apache.kafka.clients.producer.ProducerRecord}
   *
   * @param recordPopulator
   * @return
   */
  public BufferedPublisherBuilder<K, V> recordPopulator(RecordPopulator<K, V> recordPopulator) {
    this.recordPopulator = recordPopulator;
    return this;
  }

  /**
   * Used to create the {@link KafkaProducer} when {@link BufferedPublisherBuilder#kafkaProducer(KafkaProducer)} is unset
   *
   * @param kafkaProducerProperties
   * @return
   */
  public BufferedPublisherBuilder<K, V> kafkaProducerProperties(Properties kafkaProducerProperties) {
    this.kafkaProducerProperties = kafkaProducerProperties;
    return this;
  }

  /**
   * Used to create the {@link KafkaProducer} when {@link BufferedPublisherBuilder#kafkaProducer(KafkaProducer)} is unset
   *
   * @param kafkaProducerProperties
   * @return
   */
  public BufferedPublisherBuilder<K, V> kafkaProducerProperties(Map<String, String> kafkaProducerProperties) {
    this.kafkaProducerProperties = new Properties();
    this.kafkaProducerProperties.putAll(kafkaProducerProperties);
    return this;
  }

  /**
   * The {@link KafkaProducer} to use to send outbound records. When unset, it will be created automatically using {@link
   * BufferedPublisherBuilder#kafkaProducerProperties(Properties)}
   *
   * @param kafkaProducer
   * @return
   */
  public BufferedPublisherBuilder<K, V> kafkaProducer(KafkaProducer<K, V> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
    return this;
  }

  /**
   * The clock used for latency stats tracking. Defaults to {@link SystemMillisClock}
   *
   * @param clock
   * @return
   */
  public BufferedPublisherBuilder<K, V> clock(Clock clock) {
    this.clock = clock;
    return this;
  }

  /**
   * The idle strategy used when there are no messages to process from the chronicle queue. Defaults to {@link BackoffIdleStrategy}
   *
   * @param idleStrategy
   * @return
   */
  public BufferedPublisherBuilder<K, V> idleStrategy(IdleStrategy idleStrategy) {
    this.idleStrategy = idleStrategy;
    return this;
  }

  /**
   * Optional. Listener to fire events after a {@link org.apache.kafka.clients.producer.ProducerRecord} has been dispatched to the {@link KafkaProducer}
   *
   * @param sendCompleteListener
   * @return
   */
  public BufferedPublisherBuilder<K, V> sendCompleteListener(SendCompleteListener sendCompleteListener) {
    this.sendCompleteListener = sendCompleteListener;
    return this;
  }

  public BufferedPublisher build() {
    MultiStoreFileListener multiStoreFileListener = null;
    if(chronicleQueue == null) {
      multiStoreFileListener = new MultiStoreFileListener();
      if(chronicleQueuePath == null) {
        throw new IllegalArgumentException("chronicleQueue and chronicleQueuePath cannot both be null");
      }
      if(chronicleQueueRollCycle == null) {
        throw new IllegalArgumentException("chronicleQueue and chronicleQueueRollCycle cannot both be null");
      }
      if(chronicleStoreFileListener != null) {
        multiStoreFileListener.addListener(chronicleStoreFileListener);
      }
      chronicleQueue = SingleChronicleQueueBuilder.builder()
              .path(chronicleQueuePath)
              .rollCycle(chronicleQueueRollCycle)
              .storeFileListener(multiStoreFileListener)
              .build();
    }

    if(recordPopulator == null) {
      throw new IllegalArgumentException("recordPopulator cannot be null. Perhaps you could use " + DefaultRecordPopulator.class.getName());
    }

    if(kafkaProducer == null) {
      if(kafkaProducerProperties == null) {
        throw new IllegalArgumentException("kafkaProducer and kafkaProducerProperties cannot both be null");
      }
      kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
    }

    if(clock == null) {
      throw new IllegalArgumentException("clock cannot be null");
    }

    if(idleStrategy == null) {
      throw new IllegalArgumentException("idleStrategy cannot be null");
    }

    final BufferedPublisher publisher = new BufferedPublisher(
            chronicleQueue, recordPopulator, kafkaProducer, clock, idleStrategy, sendCompleteListener);

    if(chronicleQueueCleanOnRoll) {
      multiStoreFileListener.addListener(new AutoCleanupChronicleListener(publisher));
    }

    if(chronicleQueueCleanOnStartup) {
      FileUtil.deleteRecursive(new File(chronicleQueuePath));
    }

    return publisher;
  }

}
