/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import scala.collection.JavaConverters._

import org.opensearch.flint.storage.FlintOptions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FlintDataSourceV2 extends TableProvider with DataSourceRegister {

  private var table: FlintTable = null

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (table == null) {
      table = getFlintTable(Option.empty, new FlintOptions(options.asScala.toMap))
    }
    table.schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    if (table == null) {
      table = getFlintTable(Some(schema), new FlintOptions(properties.asScala.toMap))
    }
    table
  }

  protected def getFlintTable(schema: Option[StructType], option: FlintOptions): FlintTable = {
    FlintTable(option.getIndexName, SparkSession.active, option, schema)
  }

  /**
   * format name. for instance, `sql.read.format("flint")`
   */
  override def shortName(): String = "flint"

  override def supportsExternalMetadata(): Boolean = true
}
