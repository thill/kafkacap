/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.util.stats;

import io.thill.trakrj.Stats;
import io.thill.trakrj.TrakrJ;
import io.thill.trakrj.logger.Slf4jStatLogger;

/**
 * Utility methods for TrakrJ stats
 *
 * @author Eric Thill
 */
public class StatsUtil {

  private static final String SYSKEY_STATS_CONFIG = "SYSKEY_CONFIG";

  public static Stats configuredStatsOrDefault() {
    if(System.getProperty(SYSKEY_STATS_CONFIG) == null) {
      // TrakrJ config not defined -> use default Slf4jStatLogger
      return Stats.create(new Slf4jStatLogger());
    } else {
      // TrakrJ config is defined -> use default stats instance
      return TrakrJ.stats();
    }
  }
}
