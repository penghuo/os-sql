/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.storage

import org.apache.spark.sql.connector.expressions.filter.Predicate

trait FlintReaderBuilder {
  def build(): FlintReader

  def pushPredicates(predicates: Array[Predicate]): FlintReaderBuilder
}
