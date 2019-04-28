package io.thill.kafkacap.capture;

import io.thill.kafkacap.capture.callback.SendCompleteListener;
import io.thill.kafkacap.capture.populator.DefaultRecordPopulator;
import io.thill.kafkacap.capture.populator.RecordPopulator;
import io.thill.kafkacap.clock.Clock;
import io.thill.kafkacap.clock.SystemMillisClock;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class QueuedPublisherBuilder {

  private String chronicleQueuePath;
  private RollCycle chronicleQueueRollCycle = RollCycles.MINUTELY;
  private SingleChronicleQueue chronicleQueue;
  private RecordPopulator recordPopulator;
  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private Properties kafkaProducerProperties;
  private Clock clock = new SystemMillisClock();
  private IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
  private SendCompleteListener sendCompleteListener;

  /**
   * Used to create the {@link SingleChronicleQueue} when {@link QueuedPublisherBuilder#chronicleQueue(SingleChronicleQueue)} is unset
   *
   * @param chronicleQueuePath
   * @return
   */
  public QueuedPublisherBuilder chronicleQueuePath(String chronicleQueuePath) {
    this.chronicleQueuePath = chronicleQueuePath;
    return this;
  }

  /**
   * Used to create the {@link SingleChronicleQueue} when {@link QueuedPublisherBuilder#chronicleQueue(SingleChronicleQueue)} is unset
   *
   * @param chronicleQueueRollCycle
   * @return
   */
  public QueuedPublisherBuilder chronicleQueueRollCycle(RollCycle chronicleQueueRollCycle) {
    this.chronicleQueueRollCycle = chronicleQueueRollCycle;
    return this;
  }

  /**
   * The {@link SingleChronicleQueue} to use internally. When unset, it will be created automatically using {@link
   * QueuedPublisherBuilder#chronicleQueuePath(String)}
   *
   * @param chronicleQueue
   * @return
   */
  public QueuedPublisherBuilder chronicleQueue(SingleChronicleQueue chronicleQueue) {
    this.chronicleQueue = chronicleQueue;
    return this;
  }

  /**
   * Used to populate the outbound {@link org.apache.kafka.clients.producer.ProducerRecord}
   *
   * @param recordPopulator
   * @return
   */
  public QueuedPublisherBuilder recordPopulator(RecordPopulator recordPopulator) {
    this.recordPopulator = recordPopulator;
    return this;
  }

  /**
   * Used to create the {@link KafkaProducer} when {@link QueuedPublisherBuilder#kafkaProducer(KafkaProducer)} is unset
   *
   * @param kafkaProducerProperties
   * @return
   */
  public QueuedPublisherBuilder kafkaProducerProperties(Properties kafkaProducerProperties) {
    this.kafkaProducerProperties = kafkaProducerProperties;
    return this;
  }

  /**
   * The {@link KafkaProducer} to use to send outbound records. When unset, it will be created automatically using {@link
   * QueuedPublisherBuilder#kafkaProducerProperties(Properties)}
   *
   * @param kafkaProducer
   * @return
   */
  public QueuedPublisherBuilder kafkaProducer(KafkaProducer<byte[], byte[]> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
    return this;
  }

  /**
   * The clock used for latency stats tracking. Defaults to {@link SystemMillisClock}
   *
   * @param clock
   * @return
   */
  public QueuedPublisherBuilder clock(Clock clock) {
    this.clock = clock;
    return this;
  }

  /**
   * The idle strategy used when there are no messages to process from the chronicle queue. Defaults to {@link BackoffIdleStrategy}
   *
   * @param idleStrategy
   * @return
   */
  public QueuedPublisherBuilder idleStrategy(IdleStrategy idleStrategy) {
    this.idleStrategy = idleStrategy;
    return this;
  }

  /**
   * Optional. Listener to fire events after a {@link org.apache.kafka.clients.producer.ProducerRecord} has been dispatched to the {@link KafkaProducer}
   *
   * @param sendCompleteListener
   * @return
   */
  public QueuedPublisherBuilder sendCompleteListener(SendCompleteListener sendCompleteListener) {
    this.sendCompleteListener = sendCompleteListener;
    return this;
  }

  public QueuedPublisher build() {
    if(chronicleQueue == null) {
      if(chronicleQueuePath == null) {
        throw new IllegalArgumentException("chronicleQueue and chronicleQueuePath cannot both be null");
      }
      if(chronicleQueueRollCycle == null) {
        throw new IllegalArgumentException("chronicleQueue and chronicleQueueRollCycle cannot both be null");
      }
      chronicleQueue = SingleChronicleQueueBuilder.builder().path(chronicleQueuePath).rollCycle(chronicleQueueRollCycle).build();
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

    return new QueuedPublisher(chronicleQueue, recordPopulator, kafkaProducer, clock, idleStrategy, sendCompleteListener);
  }

}
