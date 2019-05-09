package io.thill.kafkacap.multicast.config;

import io.thill.kafkacap.capture.config.CaptureDeviceConfig;

public class MulticastCaptureDeviceConfig extends CaptureDeviceConfig {
  private MulticastConfig receiver;

  public MulticastConfig getReceiver() {
    return receiver;
  }

  public void setReceiver(MulticastConfig receiver) {
    this.receiver = receiver;
  }

  @Override
  public String toString() {
    return "MulticastCaptureDeviceConfig{" +
            "receiver=" + receiver +
            ", chronicle=" + getChronicle() +
            ", kafka=" + getKafka() +
            '}';
  }
}
