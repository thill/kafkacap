/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.callback;

import io.thill.kafkacap.capture.BufferedPublisherBuilder;
import io.thill.kafkacap.util.clock.Clock;
import io.thill.trakrj.Intervals;
import io.thill.trakrj.Stats;
import io.thill.trakrj.TrackerId;
import io.thill.trakrj.trackers.HistogramTracker;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Set as {@link BufferedPublisherBuilder#sendCompleteListener(SendCompleteListener)} to get <a href="https://github.com/thillio/trakrj">TrakrJ</a> Histogram
 * Stat Tracking. Latency is measured from chronicleEnqueueTime to kafkaSendReturnedTime.
 */
public class SendStatTracker<K, V> implements SendCompleteListener<K, V> {

  private final Clock clock;
  private final Stats stats;
  private final TrackerId trackerId;

  /**
   * SendStatTracker Constructor
   *
   * @param clock           The clock to use for timestamps
   * @param stats           The underlying stats instance to use for statistic records
   * @param trackerId       The tracker ID to use for latency stats
   * @param intervalSeconds The number of seconds of sampling between stat logging
   */
  public SendStatTracker(final Clock clock,
                         final Stats stats,
                         final TrackerId trackerId,
                         final int intervalSeconds) {
    this.clock = clock;
    this.stats = stats;
    this.trackerId = trackerId;
    stats.register(trackerId, new HistogramTracker(), Intervals.seconds(intervalSeconds), Intervals.seconds(intervalSeconds));
  }

  @Override
  public void onSendComplete(ProducerRecord<K, V> record, long enqueueTime) {
    final long latency = clock.now() - enqueueTime;
    stats.record(trackerId, latency);
  }
}
