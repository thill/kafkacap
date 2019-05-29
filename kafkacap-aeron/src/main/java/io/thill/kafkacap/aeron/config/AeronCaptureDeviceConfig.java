/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.aeron.config;

import io.thill.kafkacap.core.capture.config.CaptureDeviceConfig;

/**
 * AeronCaptureDeviceConfig
 *
 * @author Eric Thill
 */
public class AeronCaptureDeviceConfig extends CaptureDeviceConfig {

  private AeronReceiverConfig receiver;

  public AeronReceiverConfig getReceiver() {
    return receiver;
  }

  public void setReceiver(AeronReceiverConfig receiver) {
    this.receiver = receiver;
  }

  @Override
  public String toString() {
    return "AeronCaptureDeviceConfig{" +
            "receiver=" + receiver +
            ", chronicle=" + getChronicle() +
            ", kafka=" + getKafka() +
            '}';
  }
}
