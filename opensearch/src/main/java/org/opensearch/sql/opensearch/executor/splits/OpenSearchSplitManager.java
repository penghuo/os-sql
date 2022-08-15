/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.splits;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.splits.SplitManager;

public class OpenSearchSplitManager implements SplitManager {
  @Override
  public CompletableFuture<List<Split>> nextBatch() {
    return CompletableFuture.completedFuture(Collections.singletonList(new OpenSearchSplit()));


//    return doPrivileged(
//        () ->
//            CompletableFuture.supplyAsync(() -> ));
  }

  @Override
  public void close() {}

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }
}
