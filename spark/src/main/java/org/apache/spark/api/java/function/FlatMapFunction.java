/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A function that returns zero or more output records from each input record.
 */
@FunctionalInterface
public interface FlatMapFunction<T, R> extends Serializable {
  Iterator<R> call(T t) throws Exception;
}
