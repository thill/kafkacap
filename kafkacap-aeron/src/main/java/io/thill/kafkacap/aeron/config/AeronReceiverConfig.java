/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.aeron.config;

/**
 * AeronReceiverConfig
 *
 * @author Eric Thill
 */
public class AeronReceiverConfig {

  private String aeronDirectoryName;
  private String channel;
  private int streamId;
  private int fragmentLimit = 128;

  public String getAeronDirectoryName() {
    return aeronDirectoryName;
  }

  public void setAeronDirectoryName(String aeronDirectoryName) {
    this.aeronDirectoryName = aeronDirectoryName;
  }

  public String getChannel() {
    return channel;
  }

  public void setChannel(String channel) {
    this.channel = channel;
  }

  public int getStreamId() {
    return streamId;
  }

  public void setStreamId(int streamId) {
    this.streamId = streamId;
  }

  public int getFragmentLimit() {
    return fragmentLimit;
  }

  public void setFragmentLimit(int fragmentLimit) {
    this.fragmentLimit = fragmentLimit;
  }

  @Override
  public String toString() {
    return "AeronReceiverConfig{" +
            "aeronDirectoryName='" + aeronDirectoryName + '\'' +
            ", channel='" + channel + '\'' +
            ", streamId=" + streamId +
            ", fragmentLimit=" + fragmentLimit +
            '}';
  }
}
