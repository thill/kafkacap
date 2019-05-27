/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup;

import org.junit.Assert;
import org.junit.Test;

public class TestDeduplicatorOrdered extends AbstractDeduplicatorTest {

  @Override
  protected boolean isOrderedCapture() {
    return true;
  }

  @Test
  public void test_no_gaps() {
    sendAllTopics(PARTITION_0, 10000);
    sendAllTopics(PARTITION_0, 10001);
    sendAllTopics(PARTITION_0, 10002);

    sendAllTopics(PARTITION_1, 100);
    sendAllTopics(PARTITION_1, 101);
    sendAllTopics(PARTITION_1, 102);

    Assert.assertEquals("10000", consumer0.poll().value());
    Assert.assertEquals("10001", consumer0.poll().value());
    Assert.assertEquals("10002", consumer0.poll().value());

    Assert.assertEquals("100", consumer1.poll().value());
    Assert.assertEquals("101", consumer1.poll().value());
    Assert.assertEquals("102", consumer1.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
    Assert.assertTrue(consumer1.isEmpty());
  }

  @Test
  public void test_gap_on_lead_topic() {
    send(CAPTURE_TOPIC_A, PARTITION_0, 100);
    send(CAPTURE_TOPIC_A, PARTITION_0, 101);
    send(CAPTURE_TOPIC_A, PARTITION_0, 105);

    send(CAPTURE_TOPIC_B, PARTITION_0, 100);
    send(CAPTURE_TOPIC_B, PARTITION_0, 101);
    send(CAPTURE_TOPIC_B, PARTITION_0, 102);
    send(CAPTURE_TOPIC_B, PARTITION_0, 103);
    send(CAPTURE_TOPIC_B, PARTITION_0, 104);
    send(CAPTURE_TOPIC_B, PARTITION_0, 105);

    Assert.assertEquals("100", consumer0.poll().value());
    Assert.assertEquals("101", consumer0.poll().value());
    Assert.assertEquals("102", consumer0.poll().value());
    Assert.assertEquals("103", consumer0.poll().value());
    Assert.assertEquals("104", consumer0.poll().value());
    Assert.assertEquals("105", consumer0.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
  }

  @Test
  public void test_gap_on_follow_topic() {
    send(CAPTURE_TOPIC_A, PARTITION_0, 100);
    send(CAPTURE_TOPIC_A, PARTITION_0, 101);
    send(CAPTURE_TOPIC_A, PARTITION_0, 102);
    send(CAPTURE_TOPIC_A, PARTITION_0, 103);
    send(CAPTURE_TOPIC_A, PARTITION_0, 104);
    send(CAPTURE_TOPIC_A, PARTITION_0, 105);

    send(CAPTURE_TOPIC_B, PARTITION_0, 100);
    send(CAPTURE_TOPIC_B, PARTITION_0, 101);
    send(CAPTURE_TOPIC_B, PARTITION_0, 105);

    Assert.assertEquals("100", consumer0.poll().value());
    Assert.assertEquals("101", consumer0.poll().value());
    Assert.assertEquals("102", consumer0.poll().value());
    Assert.assertEquals("103", consumer0.poll().value());
    Assert.assertEquals("104", consumer0.poll().value());
    Assert.assertEquals("105", consumer0.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
  }

  @Test
  public void test_gap_on_all_topics() {
    send(CAPTURE_TOPIC_A, PARTITION_0, 100);
    send(CAPTURE_TOPIC_A, PARTITION_0, 101);
    send(CAPTURE_TOPIC_A, PARTITION_0, 105);

    send(CAPTURE_TOPIC_B, PARTITION_0, 100);
    send(CAPTURE_TOPIC_B, PARTITION_0, 101);
    send(CAPTURE_TOPIC_B, PARTITION_0, 105);

    send(CAPTURE_TOPIC_C, PARTITION_0, 100);
    send(CAPTURE_TOPIC_C, PARTITION_0, 101);
    send(CAPTURE_TOPIC_C, PARTITION_0, 105);

    Assert.assertEquals("100", consumer0.poll().value());

    // available immediately when gap is on all streams
    Assert.assertEquals("101", consumer0.poll().value());
    Assert.assertEquals("105", consumer0.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
  }

  @Test
  public void test_gap_on_two_active_topics() throws Exception {
    send(CAPTURE_TOPIC_A, PARTITION_0, 100);
    send(CAPTURE_TOPIC_A, PARTITION_0, 101);
    send(CAPTURE_TOPIC_A, PARTITION_0, 105);

    send(CAPTURE_TOPIC_B, PARTITION_0, 100);
    send(CAPTURE_TOPIC_B, PARTITION_0, 101);
    send(CAPTURE_TOPIC_B, PARTITION_0, 105);

    // first message2 available immediately
    Assert.assertEquals("100", consumer0.poll().value());
    Assert.assertEquals("101", consumer0.poll().value());

    // message after not available yet (must wait for timeout)
    Assert.assertTrue(consumer0.isEmpty());

    Thread.sleep(WAIT_FOR_GAP_TIME);

    // message available after timeout
    Assert.assertEquals("105", consumer0.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
  }

  @Test
  public void test_fault_tolerance() throws Exception {
    sendAllTopics(PARTITION_0, 10000);
    sendAllTopics(PARTITION_0, 10001);
    sendAllTopics(PARTITION_0, 10002);

    sendAllTopics(PARTITION_1, 100);
    sendAllTopics(PARTITION_1, 101);
    sendAllTopics(PARTITION_1, 102);

    Assert.assertEquals("10000", consumer0.poll().value());
    Assert.assertEquals("10001", consumer0.poll().value());
    Assert.assertEquals("10002", consumer0.poll().value());

    Assert.assertEquals("100", consumer1.poll().value());
    Assert.assertEquals("101", consumer1.poll().value());
    Assert.assertEquals("102", consumer1.poll().value());

    Assert.assertTrue(consumer0.isEmpty());
    Assert.assertTrue(consumer1.isEmpty());

    // stop deduplicator
    deduplicator.close();

    // send some duplicate records and next records on partition 0 while down deduplicator is down
    sendAllTopics(PARTITION_0, 10001);
    sendAllTopics(PARTITION_0, 10002);
    sendAllTopics(PARTITION_0, 10003);
    sendAllTopics(PARTITION_0, 10004);
    Thread.sleep(1000);

    // start deduplicator
    startDeduplicator();

    // send next records after restarted
    sendAllTopics(PARTITION_1, 103);
    sendAllTopics(PARTITION_1, 104);
    sendAllTopics(PARTITION_0, 10005);

    // validate no duplicates were sent and stream recovered where it should
    Assert.assertEquals("10003", consumer0.poll().value());
    Assert.assertEquals("10004", consumer0.poll().value());
    Assert.assertEquals("10005", consumer0.poll().value());

    Assert.assertEquals("103", consumer1.poll().value());
    Assert.assertEquals("104", consumer1.poll().value());
  }

}
