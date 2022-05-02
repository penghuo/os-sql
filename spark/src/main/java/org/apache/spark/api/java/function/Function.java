/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * Base interface for functions whose return types do not create special RDDs. PairFunction and
 * DoubleFunction are handled separately, to allow PairRDDs and DoubleRDDs to be constructed
 * when mapping RDDs of other types.
 */
@FunctionalInterface
public interface Function<T1, R> extends Serializable {
  R call(T1 v1) throws Exception;
}
