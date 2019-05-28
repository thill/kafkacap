/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.strategy;

/**
 * An enumeration used by {@link DedupStrategy} to flag how to handle each record
 *
 * @author Eric Thill
 */
public enum DedupResult {
  /**
   * Drop the record
   */
  DROP,

  /**
   * Send the record now
   */
  SEND,

  /**
   * Add the record to a queue to be tried again
   */
  CACHE
}
