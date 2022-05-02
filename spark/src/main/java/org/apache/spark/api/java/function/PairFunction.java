/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;
import scala.Tuple2;

/**
 * A function that returns key-value pairs (Tuple2&lt;K, V&gt;), and can be used to
 * construct PairRDDs.
 */
@FunctionalInterface
public interface PairFunction<T, K, V> extends Serializable {
  Tuple2<K, V> call(T t) throws Exception;
}
