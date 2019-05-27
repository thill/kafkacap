/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup;

import org.junit.Assert;
import org.junit.Test;

public class TestDeduplicatorUnordered extends AbstractDeduplicatorTest {

  @Override
  protected boolean isOrderedCapture() {
    return false;
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
  public void test_out_of_order_capture() {
    sendAllTopics(PARTITION_0, 100);
    sendAllTopics(PARTITION_0, 101);
    sendAllTopics(PARTITION_0, 109);
    sendAllTopics(PARTITION_0, 108);
    sendAllTopics(PARTITION_0, 107);
    sendAllTopics(PARTITION_0, 106);
    sendAllTopics(PARTITION_0, 105);
    sendAllTopics(PARTITION_0, 104);
    sendAllTopics(PARTITION_0, 103);
    sendAllTopics(PARTITION_0, 102);

    Assert.assertEquals("100", consumer0.poll().value());
    Assert.assertEquals("101", consumer0.poll().value());
    Assert.assertEquals("102", consumer0.poll().value());
    Assert.assertEquals("103", consumer0.poll().value());
    Assert.assertEquals("104", consumer0.poll().value());
    Assert.assertEquals("105", consumer0.poll().value());
    Assert.assertEquals("106", consumer0.poll().value());
    Assert.assertEquals("107", consumer0.poll().value());
    Assert.assertEquals("108", consumer0.poll().value());
    Assert.assertEquals("109", consumer0.poll().value());
  }

}
