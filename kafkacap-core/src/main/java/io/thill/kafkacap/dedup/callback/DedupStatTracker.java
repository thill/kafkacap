/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.callback;

import io.thill.kafkacap.util.clock.Clock;
import io.thill.kafkacap.util.constant.RecordHeaderKeys;
import io.thill.kafkacap.util.io.BitUtil;
import io.thill.trakrj.Intervals;
import io.thill.trakrj.Stats;
import io.thill.trakrj.TrackerId;
import io.thill.trakrj.trackers.HistogramTracker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

/**
 * A stat tracking implementation of a {@link DedupCompleteListener}. Set as {@link io.thill.kafkacap.dedup.DeduplicatorBuilder#dedupCompleteListener(DedupCompleteListener)}
 * to get <a href="https://github.com/thillio/trakrj">TrakrJ</a> Histogram Stat Tracking. Latency is measured from chronicleEnqueueTime to
 * kafkaSendReturnedTime.
 *
 * @author Eric Thill
 */
public class DedupStatTracker<K, V> implements DedupCompleteListener<K, V> {

  private final Clock clock;
  private final Stats stats;
  private final TrackerId trackerId;

  /**
   * The Constructor
   *
   * @param clock The clock implementation to generate timestamp
   * @param stats The stats instance to use
   * @param trackerId The tracker ID to use to log latency records
   * @param intervalSeconds The number of seconds of sampling between stat logging
   */
  public DedupStatTracker(final Clock clock,
                          final Stats stats,
                          final TrackerId trackerId,
                          final int intervalSeconds) {
    this.clock = clock;
    this.stats = stats;
    this.trackerId = trackerId;
    stats.register(trackerId, new HistogramTracker(), Intervals.seconds(intervalSeconds), Intervals.seconds(intervalSeconds));
  }

  @Override
  public void onDedupComplete(ConsumerRecord<K, V> consumerRecord, Headers producerHeaders) {
    final long captureQueueTime = BitUtil.bytesToLong(consumerRecord.headers().lastHeader(RecordHeaderKeys.HEADER_KEY_CAPTURE_QUEUE_TIME).value());
    final long latency = clock.now() - captureQueueTime;
    stats.record(trackerId, latency);
  }
}
