/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.aeron;

import io.aeron.Aeron;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.thill.kafkacap.aeron.config.AeronCaptureDeviceConfig;
import io.thill.kafkacap.core.capture.CaptureDevice;
import io.thill.kafkacap.core.capture.populator.RecordPopulator;
import io.thill.kafkacap.core.util.clock.Clock;
import io.thill.kafkacap.core.util.io.ResourceLoader;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SigInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;

/**
 * A {@link CaptureDevice} implementation that polls from an Aeron subscriber
 *
 * @author Eric Thill
 */
public class AeronCaptureDevice extends CaptureDevice {

  public static void main(String... args) throws IOException {
    final Logger logger = LoggerFactory.getLogger(AeronCaptureDevice.class);

    // check args
    if(args.length != 1) {
      System.err.println("Usage: AeronCaptureDevice <config>");
      logger.error("Missing Configuration Parameter");
      System.exit(1);
    }

    // load config
    logger.info("Loading config from {}...", args[0]);
    final String configStr = ResourceLoader.loadResourceOrFile(args[0]);
    logger.info("Loaded Config:\n{}", configStr);
    final AeronCaptureDeviceConfig config = new Yaml().loadAs(configStr, AeronCaptureDeviceConfig.class);
    logger.info("Parsed Config: {}", config);

    // instantiate and run
    logger.info("Instantiating {}...", AeronCaptureDevice.class.getSimpleName());
    final AeronCaptureDevice device = new AeronCaptureDevice(config);
    logger.info("Registering SigInt Handler...");
    SigInt.register(() -> {
      try {
        device.close();
      } catch(Throwable t) {
        logger.error("Close Exception", t);
      }
    });
    logger.info("Starting...");
    device.run();
    logger.info("Done");
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final InternalFragmentHandler internalFragmentHandler = new InternalFragmentHandler();
  private final ImageFragmentAssembler fragmentAssembler = new ImageFragmentAssembler(internalFragmentHandler);
  private final String aeronDirectoryName;
  private final String channel;
  private final int streamId;
  private final int fragmentLimit;

  private Aeron.Context aeronContext;
  private Aeron aeron;
  private Subscription subscription;

  public AeronCaptureDevice(AeronCaptureDeviceConfig config) {
    super(config);
    this.aeronDirectoryName = config.getReceiver().getAeronDirectoryName();
    this.channel = config.getReceiver().getChannel();
    this.streamId = config.getReceiver().getStreamId();
    this.fragmentLimit = config.getReceiver().getFragmentLimit();
  }

  @Override
  protected RecordPopulator<byte[], byte[]> createRecordPopulator(String topic, int partition, Clock clock) {
    return new AeronRecordPopulator(topic, partition, clock);
  }

  @Override
  protected void init() {
    aeronContext = new Aeron.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .availableImageHandler(img -> logger.info("Image Available: {}", img))
            .unavailableImageHandler(img -> logger.info("Image Unavailable: {}", img));
    aeron = Aeron.connect(aeronContext);
    subscription = aeron.addSubscription(channel, streamId);
    logger.info("Subscribed to channel={} streamId={}", channel, streamId);
  }

  @Override
  protected boolean poll(BufferHandler handler) {
    internalFragmentHandler.prePoll(handler);
    subscription.poll(fragmentAssembler, fragmentLimit);
    return internalFragmentHandler.polled();
  }

  @Override
  protected void cleanup() {
    subscription.close();
    aeron.close();
    aeronContext.close();
  }

  @Override
  protected void onClose() {

  }

  private static class InternalFragmentHandler implements FragmentHandler {
    private BufferHandler bufferHandler;
    private boolean polled;

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
      final byte[] payload = new byte[DataHeaderFlyweight.HEADER_LENGTH + length];
      // copy header
      buffer.getBytes(header.offset(), payload, 0, DataHeaderFlyweight.HEADER_LENGTH);
      // copy message
      buffer.getBytes(offset, payload, DataHeaderFlyweight.HEADER_LENGTH, length);
      // forward to buffer handler
      bufferHandler.handle(payload, 0, payload.length);
      // flag that something was polled/handled
      polled = true;
    }

    public void prePoll(BufferHandler bufferHandler) {
      this.bufferHandler = bufferHandler;
      this.polled = false;
    }

    public boolean polled() {
      return polled;
    }
  }



}
