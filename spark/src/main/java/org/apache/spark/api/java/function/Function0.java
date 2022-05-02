/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * A zero-argument function that returns an R.
 */
@FunctionalInterface
public interface Function0<R> extends Serializable {
  R call() throws Exception;
}
