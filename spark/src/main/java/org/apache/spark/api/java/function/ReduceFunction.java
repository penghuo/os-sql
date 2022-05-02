/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * Base interface for function used in Dataset's reduce.
 */
@FunctionalInterface
public interface ReduceFunction<T> extends Serializable {
  T call(T v1, T v2) throws Exception;
}
