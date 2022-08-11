/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.splits;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SplitManager {
  default boolean noMoreSplits() {
    return true;
  }

  CompletableFuture<List<Split>> nextBatch();

  void close();
}
