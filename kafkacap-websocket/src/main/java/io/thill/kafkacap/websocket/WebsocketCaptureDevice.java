/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.websocket;

import io.thill.kafkacap.core.capture.CaptureDevice;
import io.thill.kafkacap.core.capture.populator.DefaultRecordPopulator;
import io.thill.kafkacap.core.capture.populator.RecordPopulator;
import io.thill.kafkacap.core.util.clock.Clock;
import io.thill.kafkacap.core.util.io.ResourceLoader;
import io.thill.kafkacap.core.util.stats.StatsUtil;
import io.thill.kafkacap.websocket.config.WebsocketCaptureDeviceConfig;
import io.thill.kafkacap.websocket.internal.WebsocketClient;
import io.thill.trakrj.Stats;
import org.agrona.concurrent.SigInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;

/**
 *
 *
 * @author Eric Thill
 */
public class WebsocketCaptureDevice extends CaptureDevice<byte[], byte[]> {

  public static void main(String... args) throws IOException {
    final Logger logger = LoggerFactory.getLogger(WebsocketCaptureDevice.class);

    // check args
    if(args.length != 1) {
      System.err.println("Usage: WebsocketCaptureDevice <config>");
      logger.error("Missing Configuration Parameter");
      System.exit(1);
    }

    // load config
    logger.info("Loading config from {}...", args[0]);
    final String configStr = ResourceLoader.loadResourceOrFile(args[0]);
    logger.info("Loaded Config:\n{}", configStr);
    final WebsocketCaptureDeviceConfig config = new Yaml().loadAs(configStr, WebsocketCaptureDeviceConfig.class);
    logger.info("Parsed Config: {}", config);

    // instantiate and run
    logger.info("Instantiating {}...", WebsocketCaptureDevice.class.getSimpleName());
    final WebsocketCaptureDevice device = new WebsocketCaptureDevice(config, StatsUtil.configuredStatsOrDefault());
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

  private static final long CONNECT_FAILURE_SLEEP_MILLIS = 1000;

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final String url;
  private final String charset;
  private WebsocketClient client;

  public WebsocketCaptureDevice(WebsocketCaptureDeviceConfig config, Stats stats) throws IOException {
    super(config, stats);
    this.url = config.getReceiver().getUrl();
    this.charset = config.getReceiver().getCharset();
  }

  @Override
  protected RecordPopulator createRecordPopulator(String topic, int partition, Clock clock) {
    return new DefaultRecordPopulator<>(topic, partition, clock);
  }

  @Override
  protected void init() {

  }

  @Override
  protected boolean doWork() {
    if(client != null && client.isClosed()) {
      logger.warn("WebSocket Closed. Reconnecting...");
      client = null;
    }

    if(client == null) {
      try {
        logger.info("Connecting to {}", url);
        client = new WebsocketClient(new URI(url), Charset.forName(charset), bufferedPublisher);
        client.connect();
      } catch(Throwable t) {
        client = null;
        logger.error("Could not connect. Will retry...", t);
        sleepOnConnectFailure();
      }
      return true;
    }

    return false;
  }

  private void sleepOnConnectFailure() {
    try {
      Thread.sleep(CONNECT_FAILURE_SLEEP_MILLIS);
    } catch(InterruptedException e) {

    }
  }

  @Override
  protected void cleanup() {
    try {
      if(client != null) {
        client.close();
        client = null;
      }
    } catch(Throwable t) {
      logger.warn("Could not close WebSocket client", t);
    }
  }

  @Override
  protected void onClose() {

  }


}