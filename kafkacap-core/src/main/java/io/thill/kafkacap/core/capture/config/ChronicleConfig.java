/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.capture.config;

import net.openhft.chronicle.queue.RollCycles;

/**
 * The Chronicle portion of a {@link CaptureDeviceConfig}
 *
 * @author Eric Thill
 */
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
