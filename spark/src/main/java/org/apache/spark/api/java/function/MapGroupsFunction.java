/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Base interface for a map function used in GroupedDataset's mapGroup function.
 */
@FunctionalInterface
public interface MapGroupsFunction<K, V, R> extends Serializable {
  R call(K key, Iterator<V> values) throws Exception;
}
