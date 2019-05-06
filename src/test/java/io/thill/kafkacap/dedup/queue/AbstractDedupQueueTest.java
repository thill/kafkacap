package io.thill.kafkacap.dedup.queue;

import io.thill.kafkacap.dedup.queue.DedupQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;

public abstract class AbstractDedupQueueTest {

  private static final int PARTITION_0 = 0;
  private static final int PARTITION_1 = 1;
  private static final int NUM_TOPICS = 3;
  private static final int TOPIC_A = 0;
  private static final int TOPIC_B = 1;
  private static final int TOPIC_C = 2;

  protected abstract DedupQueue<String, String> createQueue();

  private DedupQueue<String, String> queue;

  @Before
  public void setup() {
    queue = createQueue();
    queue.assigned(new LinkedHashSet<>(Arrays.asList(PARTITION_0, PARTITION_1)), NUM_TOPICS);
  }

  @Test
  public void test_peek_and_poll() {
    Assert.assertTrue(queue.isEmpty(PARTITION_0));
    Assert.assertTrue(queue.isEmpty(PARTITION_1));

    enqueue(PARTITION_0, TOPIC_A, "R1");
    Assert.assertFalse(queue.isEmpty(PARTITION_0));
    Assert.assertTrue(queue.isEmpty(PARTITION_1));

    enqueue(PARTITION_1, TOPIC_A, "R2");
    enqueue(PARTITION_0, TOPIC_B, "R3");
    enqueue(PARTITION_0, TOPIC_B, "R4");
    Assert.assertFalse(queue.isEmpty(PARTITION_0));
    Assert.assertFalse(queue.isEmpty(PARTITION_1));

    // test peek
    Assert.assertEquals("R2", queue.peek(PARTITION_1, TOPIC_A).value());
    Assert.assertFalse(queue.isEmpty(PARTITION_0));
    Assert.assertFalse(queue.isEmpty(PARTITION_1));

    // test poll to create empty queue
    Assert.assertEquals("R2", queue.poll(PARTITION_1, TOPIC_A).value());
    Assert.assertFalse(queue.isEmpty(PARTITION_0));
    Assert.assertTrue(queue.isEmpty(PARTITION_1));
    Assert.assertNull(queue.poll(PARTITION_1, TOPIC_A));
    Assert.assertNull(queue.peek(PARTITION_1, TOPIC_A));

    Assert.assertEquals("R3", queue.poll(PARTITION_0, TOPIC_B).value());
    Assert.assertEquals("R4", queue.poll(PARTITION_0, TOPIC_B).value());
    Assert.assertFalse(queue.isEmpty(PARTITION_0));

    Assert.assertEquals("R1", queue.poll(PARTITION_0, TOPIC_A).value());
    Assert.assertTrue(queue.isEmpty(PARTITION_0));

    Assert.assertNull(queue.peek(PARTITION_0, TOPIC_A));
    Assert.assertNull(queue.peek(PARTITION_0, TOPIC_B));
    Assert.assertNull(queue.peek(PARTITION_0, TOPIC_C));
    Assert.assertNull(queue.peek(PARTITION_1, TOPIC_A));
    Assert.assertNull(queue.peek(PARTITION_1, TOPIC_B));
    Assert.assertNull(queue.peek(PARTITION_1, TOPIC_C));
  }

  private void enqueue(int partition, int topic, String value) {
    queue.add(partition, topic, new ConsumerRecord<>("topic-" + topic, partition, 0, value, value));
  }
}
