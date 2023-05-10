/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.storage.{FlintClient, FlintOptions}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

case class FlintReaderFactory(
    tableName: String,
    schema: StructType,
    option: FlintOptions,
    pushedPredicates: Array[Predicate])
    extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val flintClient = FlintClient.create(option)
    new FlintPartitionReader(
      flintClient.createReaderBuilder(tableName).pushPredicates(pushedPredicates).build(),
      schema,
      option)
  }
}
