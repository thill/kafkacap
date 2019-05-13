/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.util.clock;

public class SettableClock implements Clock {
  private volatile long now;

  @Override
  public long now() {
    return now;
  }

  public void set(long now) {
    this.now = now;
  }

}
