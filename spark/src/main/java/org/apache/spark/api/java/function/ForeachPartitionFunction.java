/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Base interface for a function used in Dataset's foreachPartition function.
 */
@FunctionalInterface
public interface ForeachPartitionFunction<T> extends Serializable {
  void call(Iterator<T> t) throws Exception;
}
