/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.capture.queue;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Buffers messages to memory using an underling {@link BlockingQueue}
 *
 * @author Eric Thill
 */
public class MemoryCaptureQueue implements CaptureQueue {

  private final BlockingQueue<QueueElement> queue;

  /**
   * Instantiate using an unbound {@link LinkedBlockingQueue}
   */
  public MemoryCaptureQueue() {
    this(new LinkedBlockingQueue<>());
  }

  /**
   * Instantiate using the given {@link BlockingQueue}
   *
   * @param queue The underling queue
   */
  public MemoryCaptureQueue(BlockingQueue<QueueElement> queue) {
    this.queue = queue;
  }

  @Override
  public void close() {
    queue.clear();
  }

  @Override
  public void add(final byte[] payload, final long enqueueTime) {
    add(payload, 0, payload.length, enqueueTime);
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
