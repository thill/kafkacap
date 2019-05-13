/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.recovery;

import io.thill.kafkacap.dedup.assignment.Assignment;

import java.util.Collection;

/**
 * Recovers state to generate an {@link Assignment}
 *
 * @param <K> The kafka record key type
 * @param <V> The kafka record value type
 * @author Eric Thill
 */
public interface RecoveryService<K, V> {
  /**
   * Generate an {@link Assignment} based on external state for the given partitions.
   *
   * @param partitions The partitions being assigned
   * @return The generated {@link Assignment}
   */
  Assignment<K, V> recover(Collection<Integer> partitions);
}
