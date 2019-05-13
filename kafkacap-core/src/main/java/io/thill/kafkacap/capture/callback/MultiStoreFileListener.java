/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.capture.callback;

import net.openhft.chronicle.queue.impl.StoreFileListener;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link StoreFileListener} that forwards all callbacks to a list of underlying {@link StoreFileListener}s
 *
 * @author Eric Thill
 */
public class MultiStoreFileListener implements StoreFileListener {

  private final List<StoreFileListener> listeners = new ArrayList<>();

  /**
   * Add a listener to the listener list
   *
   * @param listener The listener
   */
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
