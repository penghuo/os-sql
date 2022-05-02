/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.memory;

import org.apache.spark.annotation.Private;

@Private
public enum MemoryMode {
  ON_HEAP,
  OFF_HEAP
}
