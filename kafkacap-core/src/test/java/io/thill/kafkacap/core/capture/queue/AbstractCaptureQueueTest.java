/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.capture.queue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractCaptureQueueTest {

  private static final long POLL_TIMEOUT_MILLIS = 100;
  private CaptureQueue captureQueue;

  @Before
  public void setup() {
    captureQueue = createCaptureQueue();
  }

  @After
  public void teardown() {
    if(captureQueue != null) {
      captureQueue.close();
      captureQueue = null;
    }
  }

  protected abstract CaptureQueue createCaptureQueue();

  @Test
  public void test_poll_multiple_messages() {
    captureQueue.add("M1".getBytes(), 1);
    captureQueue.add("M2".getBytes(), 2);
    captureQueue.add("M3".getBytes(), 3);

    Event event;

    event = poll();
    Assert.assertEquals("M1", new String(event.buffer));
    Assert.assertEquals(1, event.queueTime);

    event = poll();
    Assert.assertEquals("M2", new String(event.buffer));
    Assert.assertEquals(2, event.queueTime);

    event = poll();
    Assert.assertEquals("M3", new String(event.buffer));
    Assert.assertEquals(3, event.queueTime);

    event = poll();
    Assert.assertNull(event);
  }

  private Event poll() {
    final AtomicReference<Event> result = new AtomicReference<>();
    final long timeout = System.currentTimeMillis() + POLL_TIMEOUT_MILLIS;
    do {
      captureQueue.poll((buffer, queueTime) -> result.set(new Event(buffer, queueTime)));
    } while(result.get() == null && System.currentTimeMillis() < timeout);
    return result.get();
  }

  private static class Event {
    private final byte[] buffer;
    private final long queueTime;
    public Event(byte[] buffer, long queueTime) {
      this.buffer = buffer;
      this.queueTime = queueTime;
    }
  }
}
