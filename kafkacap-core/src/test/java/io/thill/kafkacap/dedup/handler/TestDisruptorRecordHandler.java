package io.thill.kafkacap.dedup.handler;

import com.lmax.disruptor.BlockingWaitStrategy;
import io.thill.kafkacap.dedup.outbound.RecordSender;
import io.thill.kafkacap.dedup.queue.MemoryDedupQueue;
import io.thill.kafkacap.dedup.strategy.TestableSequencedDedupStrategy;
import io.thill.kafkacap.util.clock.SystemMillisClock;

public class TestDisruptorRecordHandler extends AbstractRecordHandlerTest {

  @Override
  protected RecordHandler<Long, String> createRecordHandler(RecordSender<Long, String> sender) {
    return new DisruptorRecordHandler<>(new SingleThreadRecordHandler<>(
            new TestableSequencedDedupStrategy(100), new MemoryDedupQueue<>(), sender, new SystemMillisClock(), null
    ), 1024, new BlockingWaitStrategy());
  }

}
