/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.multicast;

import io.thill.kafkacap.core.capture.BufferedPublisher;
import io.thill.kafkacap.core.capture.CaptureDevice;
import io.thill.kafkacap.core.capture.populator.DefaultRecordPopulator;
import io.thill.kafkacap.core.capture.populator.RecordPopulator;
import io.thill.kafkacap.core.util.clock.Clock;
import io.thill.kafkacap.core.util.stats.StatsUtil;
import io.thill.kafkacap.multicast.config.MulticastCaptureDeviceConfig;
import io.thill.kafkacap.core.util.io.ResourceLoader;
import io.thill.trakrj.Stats;
import io.thill.trakrj.TrakrJ;
import io.thill.trakrj.logger.Slf4jStatLogger;
import org.agrona.concurrent.SigInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.*;

/**
 *
 *
 * @author Eric Thill
 */
public class MulticastCaptureDevice extends CaptureDevice<byte[], byte[]> {

  public static void main(String... args) throws IOException {
    final Logger logger = LoggerFactory.getLogger(MulticastCaptureDevice.class);

    // check args
    if(args.length != 1) {
      System.err.println("Usage: MulticastCaptureDevice <config>");
      logger.error("Missing Configuration Parameter");
      System.exit(1);
    }

    // load config
    logger.info("Loading config from {}...", args[0]);
    final String configStr = ResourceLoader.loadResourceOrFile(args[0]);
    logger.info("Loaded Config:\n{}", configStr);
    final MulticastCaptureDeviceConfig config = new Yaml().loadAs(configStr, MulticastCaptureDeviceConfig.class);
    logger.info("Parsed Config: {}", config);

    // instantiate and run
    logger.info("Instantiating {}...", MulticastCaptureDevice.class.getSimpleName());
    final MulticastCaptureDevice device = new MulticastCaptureDevice(config, StatsUtil.configuredStatsOrDefault());
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
  private final InetAddress iface;
  private final InetAddress group;
  private final int port;
  private final DatagramPacket packet;
  private final MulticastSocket multicastSocket;

  public MulticastCaptureDevice(MulticastCaptureDeviceConfig config, Stats stats) throws IOException {
    super(config, stats);
    this.iface = lookupNetworkInterface(config.getReceiver().getIface());
    this.group = config.getReceiver().getGroup() == null ? null : InetAddress.getByName(config.getReceiver().getGroup());
    this.port = config.getReceiver().getPort();
    this.packet = new DatagramPacket(new byte[config.getReceiver().getMtu()], 0, config.getReceiver().getMtu());
    this.multicastSocket = new MulticastSocket(port);
  }

  private static InetAddress lookupNetworkInterface(String iface) throws SocketException, UnknownHostException {
    if(iface == null)
      return null;
    if(NetworkInterface.getByName(iface) != null)
      return NetworkInterface.getByName(iface).getInetAddresses().nextElement();
    return InetAddress.getByName(iface);
  }

  @Override
  protected RecordPopulator createRecordPopulator(String topic, int partition, Clock clock) {
    return new DefaultRecordPopulator<>(topic, partition, clock);
  }

  @Override
  protected void init() throws IOException {
    if(iface != null) {
      logger.info("Using Interface: {}", iface);
      multicastSocket.setInterface(InetAddress.getLocalHost());
    }

    logger.info("Joining Multicast Group {}:{}...", group, port);
    multicastSocket.joinGroup(group);
  }

  @Override
  protected boolean doWork() throws IOException {
    if(multicastSocket.isClosed())
      return false;
    multicastSocket.receive(packet);
    bufferedPublisher.write(packet.getData(), 0, packet.getLength());
    return true;
  }

  @Override
  protected void cleanup() {

  }

  @Override
  protected void onClose() {
    multicastSocket.close();
  }

}