package io.thill.kafkacap.capture.callback;

import net.openhft.chronicle.queue.impl.StoreFileListener;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class MultiStoreFileListener implements StoreFileListener {

  private final List<StoreFileListener> listeners = new ArrayList<>();

  public void addListener(StoreFileListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public void onAcquired(int cycle, File file) {
    for(int i = 0; i < listeners.size(); i++) {
      listeners.get(i).onAcquired(cycle, file);
    }
  }

  @Override
  public void onReleased(int cycle, File file) {
    for(int i = 0; i < listeners.size(); i++) {
      listeners.get(i).onReleased(cycle, file);
    }
  }

}
