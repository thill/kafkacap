/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.util.clock;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class SettableClock extends Clock {
  private volatile long now;

  @Override
  public long millis() {
    return now;
  }

  public void set(long now) {
    this.now = now;
  }

  /* abstract methods needed to be overridden from the Clock class */
  @Override
  public Instant instant() {
    return Instant.ofEpochMilli(now);
  }

  @Override
  public ZoneId getZone() {
    return ZoneId.systemDefault();
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return Clock.fixed(instant(), zone);
  }
}
