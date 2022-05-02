/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * Base interface for a function used in Dataset's foreach function.
 *
 * Spark will invoke the call function on each element in the input Dataset.
 */
@FunctionalInterface
public interface ForeachFunction<T> extends Serializable {
  void call(T t) throws Exception;
}
