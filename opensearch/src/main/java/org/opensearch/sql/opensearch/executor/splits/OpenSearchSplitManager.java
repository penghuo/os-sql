/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.splits;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.splits.SplitManager;

public class OpenSearchSplitManager implements SplitManager {
  @Override
  public CompletableFuture<List<Split>> nextBatch() {
    return null;
  }

  @Override
  public void close() {

  }
}
