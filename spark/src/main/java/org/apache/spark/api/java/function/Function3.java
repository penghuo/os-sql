/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * A three-argument function that takes arguments of type T1, T2 and T3 and returns an R.
 */
@FunctionalInterface
public interface Function3<T1, T2, T3, R> extends Serializable {
  R call(T1 v1, T2 v2, T3 v3) throws Exception;
}
