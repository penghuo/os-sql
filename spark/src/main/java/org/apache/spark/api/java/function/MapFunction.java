/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * Base interface for a map function used in Dataset's map function.
 */
@FunctionalInterface
public interface MapFunction<T, U> extends Serializable {
  U call(T value) throws Exception;
}
