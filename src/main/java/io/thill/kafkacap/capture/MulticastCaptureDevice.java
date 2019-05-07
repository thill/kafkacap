package io.thill.kafkacap.capture;

import io.thill.kafkacap.capture.callback.SendStatTracker;
import io.thill.kafkacap.capture.config.MulticastCaptureDeviceConfig;
import io.thill.kafkacap.util.clock.SystemMillisClock;
import io.thill.kafkacap.util.io.ResourceLoader;
import io.thill.trakrj.Stats;
import io.thill.trakrj.internal.tracker.ImmutableTrackerId;
import io.thill.trakrj.logger.Slf4jStatLogger;
import org.agrona.concurrent.SigInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class MulticastCaptureDevice implements Runnable, AutoCloseable {

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
    logger.info("Building {}...", BufferedPublisher.class.getSimpleName());
    final Stats stats = Stats.create(new Slf4jStatLogger());
    final BufferedPublisher<Void, byte[]> bufferedPublisher = new BufferedPublisherBuilder<>()
            .config(config.getPublisher())
            .sendCompleteListener(new SendStatTracker(new SystemMillisClock(), stats, new ImmutableTrackerId(0, "total_time"), 10))
            .build();
    logger.info("Instantiating {}...", MulticastCaptureDevice.class.getSimpleName());

    final MulticastCaptureDevice device = new MulticastCaptureDevice(
            lookupNetworkInterface(config.getReceiver().getIface()),
            InetAddress.getByName(config.getReceiver().getGroup()),
            config.getReceiver().getPort(),
            config.getReceiver().getMtu(),
            bufferedPublisher);
    logger.info("Registering SigInt Handler...");
    SigInt.register(() -> device.closeQuietly());
    logger.info("Starting...");
    device.run();
    logger.info("Done");
  }

  private static InetAddress lookupNetworkInterface(String iface) throws SocketException, UnknownHostException {
    if(iface == null)
      return null;
    if(NetworkInterface.getByName(iface) != null)
      return NetworkInterface.getByName(iface).getInetAddresses().nextElement();
    return InetAddress.getByName(iface);
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final CountDownLatch closeComplete = new CountDownLatch(1);
  private final InetAddress iface;
  private final InetAddress group;
  private final int port;
  private final int mtu;
  private final BufferedPublisher bufferedPublisher;

  public MulticastCaptureDevice(InetAddress iface, InetAddress group, int port, int mtu, BufferedPublisher bufferedPublisher) {
    this.iface = iface;
    this.group = group;
    this.port = port;
    this.mtu = mtu;
    this.bufferedPublisher = bufferedPublisher;
  }

  /**
   * Start the {@link MulticastCaptureDevice#run()} loop in a new thread
   */
  public void start() {
    new Thread(this, getClass().getSimpleName()).start();
  }

  @Override
  public void run() {
    MulticastSocket multicastSocket = null;

    try {
      logger.info("Starting Publisher...");
      bufferedPublisher.start();

      final DatagramPacket packet = new DatagramPacket(new byte[mtu], 0, mtu);
      multicastSocket = new MulticastSocket(port);
      if(iface != null) {
        logger.info("Using Interface: {}", iface);
        multicastSocket.setInterface(InetAddress.getLocalHost());
      }

      logger.info("Joining Multicast Group {}:{}...", group, port);
      multicastSocket.joinGroup(group);

      logger.info("Started");
      while(keepRunning.get()) {
        multicastSocket.receive(packet);
        bufferedPublisher.write(packet.getData(), 0, packet.getLength());
      }
    } catch(Throwable t) {
      logger.error("Encountered Unhandled Exception", t);
    } finally {
      logger.info("Closing Multicast Socket...");
      tryClose(multicastSocket);
      logger.info("Closing Publisher...");
      tryClose(bufferedPublisher);
      logger.info("Closed");
      closeComplete.countDown();
    }
  }

  private void tryClose(AutoCloseable closeable) {
    try {
      if(closeable != null) {
        closeable.close();
      }
    } catch(Throwable t) {
      logger.error("Could not close " + closeable.getClass().getSimpleName(), t);
    }
  }

  @Override
  public void close() throws InterruptedException {
    logger.info("Setting Close Flag...");
    keepRunning.set(false);
    logger.info("Awaiting Close Complete...");
    closeComplete.await();
    logger.info("Close Complete");
  }

  public void closeQuietly() {
    try {
      close();
    } catch(Throwable t) {
      logger.debug("Exception on close", t);
    }
  }
}