package io.thill.kafkacap.capture.queue;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

/**
 * Buffers messages to disk using <a href="https://github.com/OpenHFT/Chronicle-Queue">Chronicle-Queue</a>
 *
 * @author Eric Thill
 */
public class ChronicleCaptureQueue implements CaptureQueue {

  private final SingleChronicleQueue chronicleQueue;
  private final ExcerptAppender chronicleAppender;
  private final ExcerptTailer chronicleTailer;

  public ChronicleCaptureQueue(final SingleChronicleQueue chronicleQueue) {
    this.chronicleQueue = chronicleQueue;
    this.chronicleAppender = chronicleQueue.acquireAppender();
    this.chronicleTailer = chronicleQueue.createTailer().toEnd();
  }

  @Override
  public void close() {
    chronicleQueue.close();
  }

  @Override
  public void add(byte[] payload, long enqueueTime) {
    chronicleAppender.writeBytes(b -> {
      b.writeLong(enqueueTime);
      b.writeInt(payload.length);
      b.write(payload);
    });
  }

  @Override
  public void add(byte[] payload, int payloadOffset, int payloadLength, long enqueueTime) {
    chronicleAppender.writeBytes(b -> {
      b.writeLong(enqueueTime);
      b.writeInt(payloadLength);
      b.write(payload, payloadOffset, payloadLength);
    });
  }

  @Override
  public boolean poll(ElementHandler handler) {
    return chronicleTailer.readBytes(b -> {
      // parse payload
      final long enqueueTime = b.readLong();
      final int length = b.readInt();
      final byte[] payload = new byte[length];
      b.read(payload);
      handler.handle(payload, enqueueTime);
    });
  }

}
