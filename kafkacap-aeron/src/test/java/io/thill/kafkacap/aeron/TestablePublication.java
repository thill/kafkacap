/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.thill.kafkacap.core.util.io.FileUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * TestablePublication
 *
 * @author Eric Thill
 */
public class TestablePublication implements AutoCloseable {

  private static final long PUBLISH_TIMEOUT = 5000;

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final String channel;
  private final int streamId;

  private File aeronDirectory;
  private MediaDriver.Context driverContext;
  private MediaDriver driver;
  private Aeron.Context aeronContext;
  private Aeron aeron;
  private Publication publication;

  public TestablePublication(String channel, int streamId) {
    this.channel = channel;
    this.streamId = streamId;
  }

  public void start() throws IOException {
    // create aeron directory
    aeronDirectory = Files.createTempDirectory("aeron-test-").toFile();
    logger.info("AeronDirectory: {}", aeronDirectory.getAbsolutePath());

    // start embedded driver
    logger.info("Launching Aeron Embedded MediaDriver");
    driverContext = new MediaDriver.Context();
    driverContext.aeronDirectoryName(aeronDirectory.getAbsolutePath());
    driver = MediaDriver.launchEmbedded(driverContext);

    // start publication
    logger.info("Starting Aeron Publication");
    aeronContext = new Aeron.Context();
    aeronContext.aeronDirectoryName(aeronDirectory.getAbsolutePath());
    aeron = Aeron.connect(aeronContext);
    publication = aeron.addPublication(channel, streamId);
  }

  public void send(String message) throws IOException {
    final byte[] bytes = message.getBytes();
    final DirectBuffer buffer = new UnsafeBuffer(bytes);

    long start = System.currentTimeMillis();
    long result = -1;
    while (result < 0L) {
      result = publication.offer(buffer, 0, bytes.length);
      if(result < 0L && System.currentTimeMillis() >= start + PUBLISH_TIMEOUT)
        throw new IOException("Publication Result: " + result);
    }
  }

  public int sessionId() {
    return publication.sessionId();
  }

  @Override
  public void close() {
    tryClose(publication);
    aeron = tryClose(aeron);
    driver = tryClose(driver);
    aeronDirectory = delete(aeronDirectory);
  }

  private <T extends AutoCloseable> T tryClose(T closeable) {
    if(closeable == null)
      return null;
    try {
      logger.info("Closing {}", closeable.getClass().getSimpleName());
      closeable.close();
    } catch(Throwable t) {
      logger.error("Could not close {}", closeable.getClass().getSimpleName());
    }
    return null;
  }

  private File delete(File file) {
    if(file != null) {
      logger.info("Deleting {}", file.getAbsolutePath());
      FileUtil.deleteRecursive(file);
    }
    return null;
  }


}
