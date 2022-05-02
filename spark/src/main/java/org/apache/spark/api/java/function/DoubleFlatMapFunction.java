/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A function that returns zero or more records of type Double from each input record.
 */
@FunctionalInterface
public interface DoubleFlatMapFunction<T> extends Serializable {
  Iterator<Double> call(T t) throws Exception;
}
