/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.opensearch.sql.calcite.remote.CalciteMathematicalFunctionIT;

public class DistributedMathematicalFunctionIT extends CalciteMathematicalFunctionIT {
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
  public void testEvalComplexExpression() throws IOException {
    // Complex nested eval expression unsupported in distributed engine
    withDistributedEngineDisabled(() -> super.testEvalComplexExpression());
  }

  @Override
  public void testEvalNestedSumAvg() throws IOException {
    // Nested sum/avg in eval unsupported in distributed engine
    withDistributedEngineDisabled(() -> super.testEvalNestedSumAvg());
  }
}
