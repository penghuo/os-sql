/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 *  A function that returns Doubles, and can be used to construct DoubleRDDs.
 */
@FunctionalInterface
public interface DoubleFunction<T> extends Serializable {
  double call(T t) throws Exception;
}
