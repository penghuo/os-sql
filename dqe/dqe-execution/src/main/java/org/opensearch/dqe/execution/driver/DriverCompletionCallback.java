/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.driver;

import javax.annotation.Nullable;

/** Callback invoked when a Driver finishes execution (success or failure). */
@FunctionalInterface
public interface DriverCompletionCallback {
  void onComplete(Driver driver, @Nullable Exception failure);
}
