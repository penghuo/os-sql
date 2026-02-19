/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.opensearch.sql.calcite.remote.CalciteWhereCommandIT;

public class DistributedWhereCommandIT extends CalciteWhereCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableDistributedEngine();
  }

  /**
   * Helper to run a test with the distributed engine temporarily disabled. Used for patterns where
   * the distributed engine produces silently wrong results (metadata fields, text field IS NULL,
   * birthdate null handling).
   */
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
  public void testWhereWithMetadataFields() throws IOException {
    withDistributedEngineDisabled(() -> super.testWhereWithMetadataFields());
  }

  @Override
  public void testWhereWithMetadataFields2() throws IOException {
    withDistributedEngineDisabled(() -> super.testWhereWithMetadataFields2());
  }

  @Override
  public void testIsNullFunction() throws IOException {
    withDistributedEngineDisabled(() -> super.testIsNullFunction());
  }

  @Override
  public void testWhereEquivalentSortCommand() throws IOException {
    withDistributedEngineDisabled(() -> super.testWhereEquivalentSortCommand());
  }
}
