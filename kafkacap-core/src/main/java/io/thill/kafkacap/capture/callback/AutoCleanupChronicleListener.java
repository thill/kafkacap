/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.callback;

import io.thill.kafkacap.capture.BufferedPublisher;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * {@link StoreFileListener} that automatically deletes files once they are released
 *
 * @author Eric Thill
 */
public class AutoCleanupChronicleListener implements StoreFileListener {

  private final Logger logger = LoggerFactory.getLogger(getClass());
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
  public void onReleased(int cycle, File file) {
    logger.info("Released {}", file.getAbsolutePath());
    publisher.flush();
    if(file.delete()) {
      logger.info("Deleted {}", file.getAbsolutePath());
    }
  }

}
