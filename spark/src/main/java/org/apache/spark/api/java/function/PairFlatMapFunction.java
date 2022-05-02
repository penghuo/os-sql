/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;
import scala.Tuple2;

/**
 * A function that returns zero or more key-value pair records from each input record. The
 * key-value pairs are represented as scala.Tuple2 objects.
 */
@FunctionalInterface
public interface PairFlatMapFunction<T, K, V> extends Serializable {
  Iterator<Tuple2<K, V>> call(T t) throws Exception;
}
