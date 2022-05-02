/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A function that takes two inputs and returns zero or more output records.
 */
@FunctionalInterface
public interface FlatMapFunction2<T1, T2, R> extends Serializable {
  Iterator<R> call(T1 t1, T2 t2) throws Exception;
}
