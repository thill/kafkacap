package io.thill.kafkacap.capture.config;

import net.openhft.chronicle.queue.RollCycles;

public class ChronicleConfig {
  private String path;
  private RollCycles rollCycle;
  private boolean cleanOnRoll;
  private boolean cleanOnStartup;

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

  public boolean isCleanOnRoll() {
    return cleanOnRoll;
  }

  public void setCleanOnRoll(boolean cleanOnRoll) {
    this.cleanOnRoll = cleanOnRoll;
  }

  public boolean isCleanOnStartup() {
    return cleanOnStartup;
  }

  public void setCleanOnStartup(boolean cleanOnStartup) {
    this.cleanOnStartup = cleanOnStartup;
  }

  @Override
  public String toString() {
    return "ChronicleConfig{" +
            "path='" + path + '\'' +
            ", rollCycle=" + rollCycle +
            ", cleanOnRoll=" + cleanOnRoll +
            ", cleanOnStartup=" + cleanOnStartup +
            '}';
  }
}
