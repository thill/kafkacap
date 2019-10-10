package io.thill.kafkacap.core.dedup.config;

import java.util.Collections;
import java.util.Map;

/**
 * Configuration object for constructing a {@link io.thill.kafkacap.core.dedup.strategy.DedupStrategy}
 *
 * @author Eric Thill
 */
public class DedupStrategyConfig {
  private String impl;
  private Map<String, String> props = Collections.emptyMap();

  public String getImpl() {
    return impl;
  }

  public void setImpl(String impl) {
    this.impl = impl;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public String toString() {
    return "DedupStrategyConfig{" +
            "impl='" + impl + '\'' +
            ", props=" + props +
            '}';
  }
}
