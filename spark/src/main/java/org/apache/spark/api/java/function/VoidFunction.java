/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * A function with no return value.
 */
@FunctionalInterface
public interface VoidFunction<T> extends Serializable {
  void call(T t) throws Exception;
}
