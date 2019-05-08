package io.thill.kafkacap.capture.config;

public class MulticastConfig {
  private String iface;
  private String group;
  private int port;
  private int mtu;

  public String getIface() {
    return iface;
  }

  public void setIface(String iface) {
    this.iface = iface;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getMtu() {
    return mtu;
  }

  public void setMtu(int mtu) {
    this.mtu = mtu;
  }

  @Override
  public String toString() {
    return "MulticastConfig{" +
            "iface='" + iface + '\'' +
            ", group='" + group + '\'' +
            ", port=" + port +
            ", mtu=" + mtu +
            '}';
  }
}
