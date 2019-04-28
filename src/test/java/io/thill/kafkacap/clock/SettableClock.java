package io.thill.kafkacap.clock;

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
