/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.capture.callback;

import io.thill.kafkacap.core.capture.BufferedPublisher;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link StoreFileListener} that automatically deletes files once they are released. This assumes there is only one writer and one tailer.
 *
 * @author Eric Thill
 */
public class AutoCleanupChronicleListener implements StoreFileListener {

  private static final int NUM_REFERENCES = 2; // 1 writer and 1 tailer

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Map<Integer, Integer> referenceCounts = new LinkedHashMap<>();
  private final BufferedPublisher publisher;

  /**
   * AutoCleanupChronicleListener Constructor
   *
   * @param publisher The underlying {@link BufferedPublisher} to be flushed prior to chronicle file deletion
   */
  public AutoCleanupChronicleListener(BufferedPublisher publisher) {
    this.publisher = publisher;
  }

  @Override
  public synchronized void onAcquired(int cycle, File file) {
    if(!referenceCounts.containsKey(cycle)) {
      logger.info("Acquired {}", file.getAbsolutePath());
      referenceCounts.put(cycle, NUM_REFERENCES);
    }
  }

  @Override
  public synchronized void onReleased(int cycle, File file) {
    // decrement reference count for cycle
    referenceCounts.put(cycle, referenceCounts.getOrDefault(cycle, 1) - 1);

    if(referenceCounts.get(cycle) == 0) {
      // no more referenceCounts, delete it
      logger.info("Released {}", file.getAbsolutePath());
      try {
        publisher.flush();
      } catch(InterruptedException e) {
        logger.error("Interrupted", e);
      }
      if(file.delete()) {
        logger.info("Deleted {}", file.getAbsolutePath());
      }
    }
  }

}
