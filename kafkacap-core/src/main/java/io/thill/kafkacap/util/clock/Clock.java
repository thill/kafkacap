/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.util.clock;

/**
 * A clock that returns the current timestamp. The unit of measure is up to the implementation.
 *
 * @author Eric Thill
 */
public interface Clock {
  /**
   * The current timestamp. The unit of measure is up to the implementation.
   *
   * @return The current timestamp
   */
  long now();
}
