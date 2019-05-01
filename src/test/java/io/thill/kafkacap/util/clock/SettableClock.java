package io.thill.kafkacap.util.clock;

public class SettableClock implements Clock {
  private volatile long now;

  @Override
  public long now() {
    return now;
  }

  public void set(long now) {
    this.now = now;
  }

}
