package io.thill.kafkacap.capture.config;

public class MulticastCaptureDeviceConfig {
  private MulticastConfig receiver;
  private PublisherConfig publisher;

  public MulticastConfig getReceiver() {
    return receiver;
  }

  public void setReceiver(MulticastConfig receiver) {
    this.receiver = receiver;
  }

  public PublisherConfig getPublisher() {
    return publisher;
  }

  public void setPublisher(PublisherConfig publisher) {
    this.publisher = publisher;
  }

  @Override
  public String toString() {
    return "MulticastCaptureDeviceConfig{" +
            "receiver=" + receiver +
            ", publisher=" + publisher +
            '}';
  }
}
