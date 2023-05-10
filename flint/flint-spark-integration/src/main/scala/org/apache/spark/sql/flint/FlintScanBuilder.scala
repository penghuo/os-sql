/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.storage.FlintOptions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

case class FlintScanBuilder(
    tableName: String,
    sparkSession: SparkSession,
    schema: StructType,
    options: FlintOptions)
    extends ScanBuilder
    with SupportsPushDownV2Filters
    with Logging {

  private var pushedPredicate = Array.empty[Predicate]

  override def build(): Scan = {
    FLintScan(tableName, schema, options, pushedPredicate)
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (pushed, unSupported) =
      predicates.partition(FlintQueryBuilder.compilePredicate(_).isDefined)
    pushedPredicate = pushed
    unSupported
  }

  override def pushedPredicates(): Array[Predicate] = pushedPredicate
}
