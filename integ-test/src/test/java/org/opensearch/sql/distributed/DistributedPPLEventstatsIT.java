/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.opensearch.sql.calcite.remote.CalcitePPLEventstatsIT;

public class DistributedPPLEventstatsIT extends CalcitePPLEventstatsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableDistributedEngine();
  }

  private void withDistributedEngineDisabled(ThrowingRunnable runnable) throws IOException {
    disableDistributedEngine();
    try {
      runnable.run();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      enableDistributedEngine();
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  @Override
  public void testMultipleEventstats() throws IOException {
    // Multiple eventstats uses WindowNode which is unsupported in distributed engine
    withDistributedEngineDisabled(() -> super.testMultipleEventstats());
  }

  @Override
  public void testMultipleEventstatsWithNull() throws IOException {
    // Multiple eventstats uses WindowNode which is unsupported in distributed engine
    withDistributedEngineDisabled(() -> super.testMultipleEventstatsWithNull());
  }
}
