package io.thill.kafkacap.clock;

public class SystemMillisClock implements Clock {
  @Override
  public long now() {
    return System.currentTimeMillis();
  }
}
