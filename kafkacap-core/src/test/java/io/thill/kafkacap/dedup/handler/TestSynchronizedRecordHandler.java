/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.handler;

import io.thill.kafkacap.dedup.outbound.RecordSender;
import io.thill.kafkacap.dedup.queue.MemoryDedupQueue;
import io.thill.kafkacap.dedup.strategy.TestableSequencedDedupStrategy;
import io.thill.kafkacap.util.clock.SystemMillisClock;

public class TestSynchronizedRecordHandler extends AbstractRecordHandlerTest {

  @Override
  protected RecordHandler<Long, String> createRecordHandler(RecordSender<Long, String> sender) {
    return new SynchronizedRecordHandler<>(new SingleThreadRecordHandler<>(
            new TestableSequencedDedupStrategy(100), new MemoryDedupQueue<>(), sender, new SystemMillisClock(), null
    ));
  }

}
