/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.dedup.handler;

import io.thill.kafkacap.core.dedup.cache.MemoryRecordCache;
import io.thill.kafkacap.core.dedup.cache.RecordCacheManager;
import io.thill.kafkacap.core.dedup.outbound.RecordSender;
import io.thill.kafkacap.core.dedup.strategy.TestableSequencedDedupStrategy;
import io.thill.kafkacap.core.util.clock.SystemMillisClock;

public class TestSynchronizedRecordHandler extends AbstractRecordHandlerTest {

  private static final boolean ORDERED_CAPTURE = true;

  @Override
  protected RecordHandler<Long, String> createRecordHandler(RecordSender<Long, String> sender) {
    return new SynchronizedRecordHandler<>(new SingleThreadRecordHandler<>(
            new TestableSequencedDedupStrategy(ORDERED_CAPTURE, 100),
            new RecordCacheManager<>(MemoryRecordCache.factory()),
            ORDERED_CAPTURE, sender, new SystemMillisClock(), null
    ));
  }

}
