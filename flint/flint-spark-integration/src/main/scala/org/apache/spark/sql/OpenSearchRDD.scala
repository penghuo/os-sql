/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * Todo.
 * column: Array[String],
 * predicates: Array[Predicate],
 * limit: Int
 */
private[spark] class OpenSearchRDD(
  sc: SparkContext,
  schema: StructType,
  options: OpenSearchOptions)
  extends RDD[InternalRow](sc, Nil){

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    // ignore partition
    new OpenSearchRowIterator(schema)
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new OpenSearchPartition(0, 0))
  }
}

private[spark] class OpenSearchPartition(rddId: Int, idx: Int)
  extends Partition {

  override def hashCode(): Int = 41 * (41 * (41 + rddId) + idx)

  override val index: Int = idx
}
