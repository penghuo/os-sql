/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * A two-argument function that takes arguments of type T1 and T2 with no return value.
 */
@FunctionalInterface
public interface VoidFunction2<T1, T2> extends Serializable {
  void call(T1 v1, T2 v2) throws Exception;
}
