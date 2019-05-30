/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.websocket.config;

import io.thill.kafkacap.core.capture.config.CaptureDeviceConfig;

public class WebsocketCaptureDeviceConfig extends CaptureDeviceConfig {
  private WebsocketConfig receiver;

  public WebsocketConfig getReceiver() {
    return receiver;
  }

  public void setReceiver(WebsocketConfig receiver) {
    this.receiver = receiver;
  }

  @Override
  public String toString() {
    return "WebsocketCaptureDeviceConfig{" +
            "receiver=" + receiver +
            ", chronicle=" + getChronicle() +
            ", kafka=" + getKafka() +
            '}';
  }
}
