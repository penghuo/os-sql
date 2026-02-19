/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.opensearch.sql.calcite.remote.CalcitePPLBasicIT;

public class DistributedPPLBasicIT extends CalcitePPLBasicIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableDistributedEngine();
    enableStrictMode();
  }

  /**
   * Helper to run a test with the distributed engine temporarily disabled. Used for multi-index and
   * wildcard patterns not supported in the distributed engine.
   */
  private void withDistributedEngineDisabled(ThrowingRunnable runnable) throws Exception {
    disableDistributedEngine();
    disableStrictMode();
    try {
      runnable.run();
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
  public void testMultipleSourceQuery_SameTable() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testMultipleSourceQuery_SameTable());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testMultipleSourceQuery_DifferentTables() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testMultipleSourceQuery_DifferentTables());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testMultipleTables_SameTable() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testMultipleTables_SameTable());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testMultipleTables_DifferentTables() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testMultipleTables_DifferentTables());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testMultipleTables_WithIndexPattern() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testMultipleTables_WithIndexPattern());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testMultipleTablesAndFilters_SameTable() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testMultipleTablesAndFilters_SameTable());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testMultipleTablesAndFilters_WithIndexPattern() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testMultipleTablesAndFilters_WithIndexPattern());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testIndexPatterns() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testIndexPatterns());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testFieldsMergedObject() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testFieldsMergedObject());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testQueryMinusFields() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testQueryMinusFields());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testQueryMinusFieldsWithFilter() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testQueryMinusFieldsWithFilter());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void testNumericLiteral() throws IOException {
    try {
      withDistributedEngineDisabled(() -> super.testNumericLiteral());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
