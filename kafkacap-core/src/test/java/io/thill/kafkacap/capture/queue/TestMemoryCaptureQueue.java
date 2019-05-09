package io.thill.kafkacap.capture.queue;

public class TestMemoryCaptureQueue extends AbstractCaptureQueueTest {

  @Override
  protected CaptureQueue createCaptureQueue() {
    return new MemoryCaptureQueue();
  }

}
