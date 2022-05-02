/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * Base interface for a function used in Dataset's filter function.
 *
 * If the function returns true, the element is included in the returned Dataset.
 */
@FunctionalInterface
public interface FilterFunction<T> extends Serializable {
  boolean call(T value) throws Exception;
}
