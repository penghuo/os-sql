/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Base interface for function used in Dataset's mapPartitions.
 */
@FunctionalInterface
public interface MapPartitionsFunction<T, U> extends Serializable {
  Iterator<U> call(Iterator<T> input) throws Exception;
}
