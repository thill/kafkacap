/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.recovery;

import io.thill.kafkacap.dedup.assignment.Assignment;

import java.util.Collection;

public interface RecoveryService<K, V> {
  Assignment<K, V> recover(Collection<Integer> partitions);
}
