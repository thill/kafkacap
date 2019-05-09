package io.thill.kafkacap.capture.config;

import net.openhft.chronicle.queue.RollCycles;

public class ChronicleConfig {
  private String path;
  private RollCycles rollCycle;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public RollCycles getRollCycle() {
    return rollCycle;
  }

  public void setRollCycle(RollCycles rollCycle) {
    this.rollCycle = rollCycle;
  }

  @Override
  public String toString() {
    return "ChronicleConfig{" +
            "path='" + path + '\'' +
            ", rollCycle=" + rollCycle +
            '}';
  }
}
