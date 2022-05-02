/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A function that returns zero or more output records from each grouping key and its values from 2
 * Datasets.
 */
@FunctionalInterface
public interface CoGroupFunction<K, V1, V2, R> extends Serializable {
  Iterator<R> call(K key, Iterator<V1> left, Iterator<V2> right) throws Exception;
}
