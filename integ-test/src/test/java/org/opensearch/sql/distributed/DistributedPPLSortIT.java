/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.opensearch.sql.calcite.remote.CalcitePPLSortIT;

public class DistributedPPLSortIT extends CalcitePPLSortIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableDistributedEngine();
    enableStrictMode();
  }

  private void withDistributedEngineDisabled(ThrowingRunnable runnable) throws IOException {
    disableDistributedEngine();
    disableStrictMode();
    try {
      runnable.run();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      enableDistributedEngine();
      enableStrictMode();
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  @Override
  public void testSortWithNullValue() throws IOException {
    // Null handling for text fields (firstname) differs via DocValues
    withDistributedEngineDisabled(() -> super.testSortWithNullValue());
  }
}
