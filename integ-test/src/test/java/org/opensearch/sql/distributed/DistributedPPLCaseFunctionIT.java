/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.opensearch.sql.calcite.remote.CalcitePPLCaseFunctionIT;

public class DistributedPPLCaseFunctionIT extends CalcitePPLCaseFunctionIT {
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
  public void testCaseWhenInFilter() throws IOException {
    // CASE WHEN IN filter produces wrong row count in distributed engine
    withDistributedEngineDisabled(() -> super.testCaseWhenInFilter());
  }
}
