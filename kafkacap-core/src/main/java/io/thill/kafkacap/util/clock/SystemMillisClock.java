/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.util.clock;

/**
 * A @{@link Clock} implementation that uses {@link System#currentTimeMillis()}
 *
 * @author Eric Thill
 */
public class SystemMillisClock implements Clock {
  @Override
  public long now() {
    return System.currentTimeMillis();
  }
}
