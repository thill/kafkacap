/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.cache;

import io.thill.kafkacap.core.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

public abstract class AbstractOrderedRecordCacheTest {

  private static final int PARTITION_0 = 0;
  private static final int PARTITION_1 = 1;
  private static final int NUM_TOPICS = 3;
  private static final int TOPIC_A = 0;
  private static final int TOPIC_B = 1;
  private static final int TOPIC_C = 2;

  protected abstract RecordCacheFactory<String, String> createFactory();

  private RecordCacheManager<String, String> cache;

  @Before
  public void setup() {
    cache = new RecordCacheManager<>(createFactory());
    cache.assigned(new Assignment<>(new LinkedHashSet<>(Arrays.asList(PARTITION_0, PARTITION_1)), NUM_TOPICS, Collections.emptyMap(), Collections.emptyMap()));
  }

  @Test
  public void test_peek_and_poll() {
    Assert.assertTrue(cache.isEmpty(PARTITION_0));
    Assert.assertTrue(cache.isEmpty(PARTITION_1));

    enqueue(PARTITION_0, TOPIC_A, "R1");
    Assert.assertFalse(cache.isEmpty(PARTITION_0));
    Assert.assertTrue(cache.isEmpty(PARTITION_1));

    enqueue(PARTITION_1, TOPIC_A, "R2");
    enqueue(PARTITION_0, TOPIC_B, "R3");
    enqueue(PARTITION_0, TOPIC_B, "R4");
    Assert.assertFalse(cache.isEmpty(PARTITION_0));
    Assert.assertFalse(cache.isEmpty(PARTITION_1));

    // test peek
    Assert.assertEquals("R2", cache.peek(PARTITION_1, TOPIC_A).value());
    Assert.assertFalse(cache.isEmpty(PARTITION_0));
    Assert.assertFalse(cache.isEmpty(PARTITION_1));

    // test poll to create empty queue
    Assert.assertEquals("R2", cache.poll(PARTITION_1, TOPIC_A).value());
    Assert.assertFalse(cache.isEmpty(PARTITION_0));
    Assert.assertTrue(cache.isEmpty(PARTITION_1));
    Assert.assertNull(cache.poll(PARTITION_1, TOPIC_A));
    Assert.assertNull(cache.peek(PARTITION_1, TOPIC_A));

    Assert.assertEquals("R3", cache.poll(PARTITION_0, TOPIC_B).value());
    Assert.assertEquals("R4", cache.poll(PARTITION_0, TOPIC_B).value());
    Assert.assertFalse(cache.isEmpty(PARTITION_0));

    Assert.assertEquals("R1", cache.poll(PARTITION_0, TOPIC_A).value());
    Assert.assertTrue(cache.isEmpty(PARTITION_0));

    Assert.assertNull(cache.peek(PARTITION_0, TOPIC_A));
    Assert.assertNull(cache.peek(PARTITION_0, TOPIC_B));
    Assert.assertNull(cache.peek(PARTITION_0, TOPIC_C));
    Assert.assertNull(cache.peek(PARTITION_1, TOPIC_A));
    Assert.assertNull(cache.peek(PARTITION_1, TOPIC_B));
    Assert.assertNull(cache.peek(PARTITION_1, TOPIC_C));
  }

  private void enqueue(int partition, int topic, String value) {
    cache.add(partition, topic, new ConsumerRecord<>("topic-" + topic, partition, 0, value, value));
  }
}
