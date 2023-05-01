/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

case class OpenSearchRelation(
  options: OpenSearchOptions,
  userSchema: Option[StructType] = None)(@transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with Logging {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]) = {
    new OpenSearchRDD(sparkSession.sparkContext, userSchema.get, options).asInstanceOf[RDD[Row]]
  }

  /**
   * Todo, fetch schema from cluster.
   */
  override def schema: StructType = userSchema.get
}
