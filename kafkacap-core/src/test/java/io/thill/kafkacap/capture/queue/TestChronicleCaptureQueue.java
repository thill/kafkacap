package io.thill.kafkacap.capture.queue;

import com.google.common.io.Files;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public class TestChronicleCaptureQueue extends AbstractCaptureQueueTest {

  @Override
  protected CaptureQueue createCaptureQueue() {
    SingleChronicleQueue chronicleQueue = SingleChronicleQueueBuilder.builder()
            .path(Files.createTempDir().getAbsolutePath())
            .rollCycle(RollCycles.TEST_SECONDLY)
            .build();
    return new ChronicleCaptureQueue(chronicleQueue);
  }

}
