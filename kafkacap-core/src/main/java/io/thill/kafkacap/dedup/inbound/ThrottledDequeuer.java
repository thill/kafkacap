/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.dedup.inbound;

import io.thill.kafkacap.dedup.handler.RecordHandler;
import org.agrona.collections.IntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Calls {@link RecordHandler#checkCache(int)} periodically based on the current {@link io.thill.kafkacap.dedup.assignment.Assignment}
 */
public class ThrottledDequeuer implements Runnable, AutoCloseable {

  private static final Duration INTERVAL_DURACTION = Duration.ofSeconds(1);
  private static final Duration ASSIGN_SLEEP_DURACTION = Duration.ofMillis(10);

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final CountDownLatch closeComplete = new CountDownLatch(1);
  private final AtomicReference<Collection<Integer>> nextAssignment = new AtomicReference<>();

  private final RecordHandler<?, ?> handler;

  /**
   * ThrottledDequeuer Constructor
   *
   * @param handler The handler used to dispatch all tryDequeue events
   */
  public ThrottledDequeuer(RecordHandler<?, ?> handler) {
    this.handler = handler;
  }

  /**
   * Start the run loop in a new thread
   */
  public void start() {
    new Thread(this, "ThrottledDequeuer").start();
  }

  @Override
  public void run() {
    IntArrayList currentAssignment = new IntArrayList();
    try {
      logger.info("Starting {}", getClass().getSimpleName());
      logger.info("Entering Poll Loop");
      while(keepRunning.get()) {
        // throttle
        Thread.sleep(INTERVAL_DURACTION.toMillis());

        // check for new assignment
        Collection<Integer> newAssignment = nextAssignment.getAndSet(null);
        if(newAssignment != null) {
          currentAssignment = new IntArrayList();
          currentAssignment.addAll(newAssignment);
          logger.info("New Assignment: {}", newAssignment);
        }

        for(int i = 0; i < currentAssignment.size(); i++) {
          final int partition = currentAssignment.getInt(i);
          handler.checkCache(partition);
        }
      }
    } catch(Throwable t) {
      logger.error("Unhandled Exception", t);
    } finally {
      logger.info("Closing...");
      closeComplete.countDown();
      logger.info("Close Complete");
    }
  }

  @Override
  public void close() throws InterruptedException {
    keepRunning.set(false);
    closeComplete.await();
  }

  public void assign(Collection<Integer> partitions) throws InterruptedException {
    nextAssignment.set(partitions);
    // block until assignment is accepted
    while(nextAssignment.get() != null) {
      Thread.sleep(ASSIGN_SLEEP_DURACTION.toMillis());
    }
  }

  public void revoke(Collection<Integer> partitions) throws InterruptedException {
    nextAssignment.set(Collections.emptyList());
    // block until assignment is accepted
    while(nextAssignment.get() != null) {
      Thread.sleep(ASSIGN_SLEEP_DURACTION.toMillis());
    }
  }

  @Override
  public String toString() {
    return "ThrottledDequeuer";
  }
}
