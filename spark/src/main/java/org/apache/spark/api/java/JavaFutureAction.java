/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java;


import java.util.List;
import java.util.concurrent.Future;

public interface JavaFutureAction<T> extends Future<T> {

  /**
   * Returns the job IDs run by the underlying async operation.
   *
   * This returns the current snapshot of the job list. Certain operations may run multiple
   * jobs, so multiple calls to this method may return different lists.
   */
  List<Integer> jobIds();
}
