package io.thill.kafkacap.dedup.handler;

import io.thill.kafkacap.dedup.handler.TestableRecordSender.TestRecord;
import io.thill.kafkacap.dedup.queue.MemoryDedupQueue;
import io.thill.kafkacap.dedup.assignment.Assignment;
import io.thill.kafkacap.dedup.strategy.TestableSequencedDedupStrategy;
import io.thill.kafkacap.util.clock.SystemMillisClock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class TestSynchronizedRecordHandler {

  private static final int PARTITION_0 = 0;
  private static final int PARTITION_1 = 1;
  private static final int TOPIC_0 = 0;
  private static final int TOPIC_1 = 1;
  private static final int TOPIC_2 = 2;
  private static final int NUM_TOPICS = 3;

  private RecordHandler<String, String> handler;
  private TestableRecordSender sender;

  @Before
  public void setup() {
    sender = new TestableRecordSender();
    handler = new SynchronizedRecordHandler<>(new TestableSequencedDedupStrategy(100), new MemoryDedupQueue<>(), sender, new SystemMillisClock(), null);
    handler.assigned(new Assignment<>(Arrays.asList(PARTITION_0, PARTITION_1), NUM_TOPICS));
  }

  @Test
  public void test_one_partition_one_topic_no_gaps() throws Exception {
    handle(PARTITION_0, 100, TOPIC_0);
    handle(PARTITION_0, 101, TOPIC_0);
    handle(PARTITION_0, 102, TOPIC_0);
    handle(PARTITION_0, 103, TOPIC_0);

    Assert.assertEquals("100", sender.poll().value());
    Assert.assertEquals("101", sender.poll().value());
    Assert.assertEquals("102", sender.poll().value());
    Assert.assertEquals("103", sender.poll().value());

    Assert.assertEquals(true, sender.isEmpty());
  }

  @Test
  public void test_one_partition_two_topics_no_gaps() throws Exception {
    handle(PARTITION_0, 100, TOPIC_0);
    handle(PARTITION_0, 101, TOPIC_0);

    handle(PARTITION_0, 100, TOPIC_1);
    handle(PARTITION_0, 101, TOPIC_1);
    handle(PARTITION_0, 102, TOPIC_1);
    handle(PARTITION_0, 103, TOPIC_1);

    handle(PARTITION_0, 102, TOPIC_0);
    handle(PARTITION_0, 103, TOPIC_0);

    Assert.assertEquals("100", sender.poll().value());
    Assert.assertEquals("101", sender.poll().value());
    Assert.assertEquals("102", sender.poll().value());
    Assert.assertEquals("103", sender.poll().value());

    Assert.assertEquals(true, sender.isEmpty());
  }

  @Test
  public void test_one_partition_two_topics_with_fillable_gaps() throws Exception {
    handle(PARTITION_0, 100, TOPIC_0);
    handle(PARTITION_0, 103, TOPIC_0);

    handle(PARTITION_0, 100, TOPIC_1);
    handle(PARTITION_0, 101, TOPIC_1);
    handle(PARTITION_0, 102, TOPIC_1);

    handler.tryDequeue(PARTITION_0);

    Assert.assertEquals("100", sender.poll().value());
    Assert.assertEquals("101", sender.poll().value());
    Assert.assertEquals("102", sender.poll().value());
    Assert.assertEquals("103", sender.poll().value());

    Assert.assertEquals(true, sender.isEmpty());
  }

  @Test
  public void test_one_partition_two_topics_gap_timeout() throws Exception {
    handle(PARTITION_0, 100, TOPIC_0);
    handle(PARTITION_0, 103, TOPIC_0);
    handle(PARTITION_0, 104, TOPIC_0);
    handle(PARTITION_0, 105, TOPIC_0);

    handle(PARTITION_0, 100, TOPIC_1);
    handle(PARTITION_0, 103, TOPIC_1);
    handle(PARTITION_0, 104, TOPIC_1);
    handle(PARTITION_0, 105, TOPIC_1);

    // first record should have been sent
    Assert.assertEquals("100", sender.poll().value());

    // wait more than timeout, then try dequeue
    Thread.sleep(101);
    handler.tryDequeue(PARTITION_0);

    // records after gap should have been sent
    Assert.assertEquals("103", sender.poll().value());
    Assert.assertEquals("104", sender.poll().value());
    Assert.assertEquals("105", sender.poll().value());

    Assert.assertEquals(true, sender.isEmpty());
  }

  @Test
  public void test_multiple_partitions_multiple_topics() throws Exception {
    handle(PARTITION_0, 100, TOPIC_0);
    handle(PARTITION_0, 101, TOPIC_0);
    handle(PARTITION_0, 102, TOPIC_0);
    handle(PARTITION_0, 103, TOPIC_0);

    handle(PARTITION_1, 100, TOPIC_0);
    handle(PARTITION_1, 101, TOPIC_0);
    handle(PARTITION_1, 102, TOPIC_0);
    handle(PARTITION_1, 103, TOPIC_0);

    handle(PARTITION_0, 100, TOPIC_1);
    handle(PARTITION_0, 101, TOPIC_1);
    handle(PARTITION_0, 102, TOPIC_1);
    handle(PARTITION_0, 103, TOPIC_1);

    handle(PARTITION_1, 100, TOPIC_1);
    handle(PARTITION_1, 101, TOPIC_1);
    handle(PARTITION_1, 102, TOPIC_1);
    handle(PARTITION_1, 103, TOPIC_1);

    TestRecord r;

    r = sender.poll();
    Assert.assertEquals("100", r.value());
    Assert.assertEquals(0, r.partition());
    r = sender.poll();
    Assert.assertEquals("101", r.value());
    Assert.assertEquals(0, r.partition());
    r = sender.poll();
    Assert.assertEquals("102", r.value());
    Assert.assertEquals(0, r.partition());
    r = sender.poll();
    Assert.assertEquals("103", r.value());
    Assert.assertEquals(0, r.partition());

    r = sender.poll();
    Assert.assertEquals("100", r.value());
    Assert.assertEquals(1, r.partition());
    r = sender.poll();
    Assert.assertEquals("101", r.value());
    Assert.assertEquals(1, r.partition());
    r = sender.poll();
    Assert.assertEquals("102", r.value());
    Assert.assertEquals(1, r.partition());
    r = sender.poll();
    Assert.assertEquals("103", r.value());
    Assert.assertEquals(1, r.partition());

    Assert.assertEquals(true, sender.isEmpty());
  }


  @Test
  public void test_gap_on_all_topics() throws Exception {
    handle(PARTITION_0, 100, TOPIC_0);
    handle(PARTITION_0, 103, TOPIC_0);
    handle(PARTITION_0, 104, TOPIC_0);
    handle(PARTITION_0, 105, TOPIC_0);

    handle(PARTITION_0, 100, TOPIC_1);
    handle(PARTITION_0, 103, TOPIC_1);
    handle(PARTITION_0, 104, TOPIC_1);
    handle(PARTITION_0, 105, TOPIC_1);

    handle(PARTITION_0, 100, TOPIC_2);
    handle(PARTITION_0, 103, TOPIC_2);
    handle(PARTITION_0, 104, TOPIC_2);
    handle(PARTITION_0, 105, TOPIC_2);

    // should dequeue immediately without a timeout since gap happened on all streams
    handler.tryDequeue(PARTITION_0);

    Assert.assertEquals("100", sender.poll().value());
    Assert.assertEquals("103", sender.poll().value());
    Assert.assertEquals("104", sender.poll().value());
    Assert.assertEquals("105", sender.poll().value());

    Assert.assertEquals(true, sender.isEmpty());
  }


  private void handle(int partition, long sequence, int topicIdx) {
    handler.handle(new ConsumerRecord<>("topic" + topicIdx, partition, 0, Long.toString(sequence), Long.toString(sequence)), topicIdx);
  }

}
