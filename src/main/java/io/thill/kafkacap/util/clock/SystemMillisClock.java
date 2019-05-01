package io.thill.kafkacap.util.clock;

public class SystemMillisClock implements Clock {
  @Override
  public long now() {
    return System.currentTimeMillis();
  }
}
