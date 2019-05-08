package io.thill.kafkacap.dedup.strategy;

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
  QUEUE
}
