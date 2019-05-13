/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.queue;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MemoryCaptureQueue implements CaptureQueue {

  private final BlockingQueue<QueueElement> queue;

  public MemoryCaptureQueue() {
    this(new LinkedBlockingQueue<>());
  }

  public MemoryCaptureQueue(BlockingQueue<QueueElement> queue) {
    this.queue = queue;
  }

  @Override
  public void close() {
    queue.clear();
  }

  @Override
  public void add(final byte[] payload, final long enqueueTime) {
    queue.add(new QueueElement(Arrays.copyOf(payload, payload.length), enqueueTime));
  }

  @Override
  public void add(byte[] payload, int payloadOffset, int payloadLength, long enqueueTime) {
    queue.add(new QueueElement(Arrays.copyOfRange(payload, payloadOffset, payloadOffset + payloadLength), enqueueTime));
  }

  @Override
  public boolean poll(final ElementHandler handler) {
    final QueueElement element = queue.poll();
    if(element == null)
      return false;
    handler.handle(element.payload, element.queueTime);
    return true;
  }

  private static class QueueElement {
    private final byte[] payload;
    private final long queueTime;

    public QueueElement(final byte[] payload, final long queueTime) {
      this.queueTime = queueTime;
      this.payload = payload;
    }
  }
}
