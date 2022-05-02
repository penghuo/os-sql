/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * A four-argument function that takes arguments of type T1, T2, T3 and T4 and returns an R.
 */
@FunctionalInterface
public interface Function4<T1, T2, T3, T4, R> extends Serializable {
  R call(T1 v1, T2 v2, T3 v3, T4 v4) throws Exception;
}
