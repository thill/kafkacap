package io.thill.kafkacap.dedup.recovery;

import io.thill.kafkacap.dedup.assignment.Assignment;

import java.util.Collection;

public interface RecoveryService<K, V> {
  Assignment<K, V> recover(Collection<Integer> partitions);
}
