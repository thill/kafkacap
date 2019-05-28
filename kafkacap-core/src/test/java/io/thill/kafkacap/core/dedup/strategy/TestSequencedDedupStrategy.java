/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.strategy;

import io.thill.kafkacap.core.dedup.assignment.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;

public class TestSequencedDedupStrategy {

  private static final String TOPIC_A = "TOPIC_A";
  private static final String TOPIC_B = "TOPIC_B";
  private static final String TOPIC_C = "TOPIC_C";

  private static final int PARITION_0 = 0;
  private static final int PARITION_1 = 1;

  private TestableSequencedDedupStrategy strategy;

  @Before
  public void setup() {
    strategy = new TestableSequencedDedupStrategy(true, 1000);
    strategy.assigned(new Assignment<>(new TreeSet<>(Arrays.asList(PARITION_0, PARITION_1)), 3, Collections.emptyMap(), Collections.emptyMap()));
  }

  @Test
  public void test_one_partition_one_topic_no_duplicates() {
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 2)));

    Assert.assertNull(strategy.getLastGapPartition());
  }

  @Test
  public void test_one_partition_one_topic_with_duplicates() {
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 2)));

    Assert.assertNull(strategy.getLastGapPartition());
  }

  @Test
  public void test_one_partition_one_topic_nonzero_first_sequence() {
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 100)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 101)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 102)));

    Assert.assertNull(strategy.getLastGapPartition());
  }

  @Test
  public void test_one_partition_all_topics_no_gaps() {
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 2)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 2)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_B, PARITION_0, 3)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_0, 3)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 4)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 2)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 3)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 4)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_C, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 5)));

    Assert.assertNull(strategy.getLastGapPartition());
  }

  @Test
  public void test_one_partition_all_topics_gap_on_one_topic() {
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 1)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 1)));
    // GAP
    Assert.assertEquals(DedupResult.CACHE, strategy.check(record(TOPIC_B, PARITION_0, 4)));

    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 2)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 3)));

    // reprocess queued message
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_B, PARITION_0, 4)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_0, 3)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_0, 4)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 2)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 3)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 4)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_C, PARITION_0, 5)));

    Assert.assertNull(strategy.getLastGapPartition());
  }

  @Test
  public void test_one_partition_all_topics_gap_on_all_topics() {
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.CACHE, strategy.check(record(TOPIC_A, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.CACHE, strategy.check(record(TOPIC_B, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 1)));

    // gap on all partitions: should send now
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_C, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_B, PARITION_0, 6)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_0, 6)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 6)));

    Assert.assertEquals(0, (int)strategy.getLastGapPartition());
    Assert.assertEquals(2, (long)strategy.getLastGapFromSequence());
    Assert.assertEquals(4, (long)strategy.getLastGapToSequence());
  }

  @Test
  public void test_one_partition_two_topics_gap_timeout() throws InterruptedException {
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.CACHE, strategy.check(record(TOPIC_A, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.CACHE, strategy.check(record(TOPIC_B, PARITION_0, 5)));

    // immediate call should CACHE
    Assert.assertEquals(DedupResult.CACHE, strategy.check(record(TOPIC_B, PARITION_0, 5)));

    // wait through timeout
    Thread.sleep(1100);

    // this call will queue, but trigger the timeout for the next call to complete
    // this is necessary to simplify logic when multiple partitions are missing different sets of sequences
    Assert.assertEquals(DedupResult.CACHE, strategy.check(record(TOPIC_B, PARITION_0, 5)));

    // same call immediately should SEND
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_B, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_0, 5)));

    Assert.assertEquals(0, (int)strategy.getLastGapPartition());
    Assert.assertEquals(2, (long)strategy.getLastGapFromSequence());
    Assert.assertEquals(4, (long)strategy.getLastGapToSequence());
  }

  @Test
  public void test_two_partitions_one_topic_no_duplicates() {
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 2)));

    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_1, 0)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_1, 1)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_1, 2)));

    Assert.assertNull(strategy.getLastGapPartition());
  }

  @Test
  public void test_two_partitions_all_topics_no_duplicates() {
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 2)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 2)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_B, PARITION_0, 3)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_0, 3)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_0, 4)));


    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_1, 0)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_1, 1)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_1, 2)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_1, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_1, 1)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_1, 2)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_B, PARITION_1, 3)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_1, 3)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_A, PARITION_1, 4)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_1, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_1, 1)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_1, 2)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_1, 3)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_1, 4)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_C, PARITION_1, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_1, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_1, 5)));


    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 0)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 1)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 2)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 3)));
    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_C, PARITION_0, 4)));
    Assert.assertEquals(DedupResult.SEND, strategy.check(record(TOPIC_C, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_A, PARITION_0, 5)));

    Assert.assertEquals(DedupResult.DROP, strategy.check(record(TOPIC_B, PARITION_0, 5)));

    Assert.assertNull(strategy.getLastGapPartition());
  }

  private ConsumerRecord<Long, String> record(String topic, int partition, long sequence) {
    return new ConsumerRecord<>(topic, partition, 0, sequence, Long.toString(sequence));
  }

}
