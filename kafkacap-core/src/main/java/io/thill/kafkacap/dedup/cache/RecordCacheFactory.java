/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.cache;

/**
 * Factory of {@link RecordCache}
 *
 * @author Eric Thill
 */
public interface RecordCacheFactory<K, V> {
  /**
   * Create, but to not start a {@link RecordCache}
   *
   * @return The created {@link RecordCache}
   */
  RecordCache<K, V> create();
}
