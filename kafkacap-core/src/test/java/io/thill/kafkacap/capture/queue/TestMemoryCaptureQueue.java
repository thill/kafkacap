/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.queue;

public class TestMemoryCaptureQueue extends AbstractCaptureQueueTest {

  @Override
  protected CaptureQueue createCaptureQueue() {
    return new MemoryCaptureQueue();
  }

}
