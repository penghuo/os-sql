/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util
import java.util.TimeZone

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.opensearch.flint.core.storage.FlintWriter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.flint.FlintPartitionWriter.ID_NAME
import org.apache.spark.sql.flint.json.FlintJacksonGenerator
import org.apache.spark.sql.types.StructType

/**
 * Submit create(put if absent) bulk request using FlintWriter. Using "create" action to avoid
 * delete-create docs.
 */
case class FlintPartitionWriter(
    flintWriter: FlintWriter,
    dataSchema: StructType,
    properties: util.Map[String, String])
    extends DataWriter[InternalRow] {

  private lazy val jsonOptions = {
    new JSONOptions(CaseInsensitiveMap(Map.empty[String, String]), TimeZone.getDefault.getID, "")
  }
  private lazy val gen = FlintJacksonGenerator(dataSchema, flintWriter, jsonOptions)

  private lazy val idOrdinal = properties.asScala.toMap
    .get(ID_NAME)
    .flatMap(filedName => dataSchema.getFieldIndex(filedName))

  /**
   * { "create": { "_id": "tt1392214" } } { "title": "Prisoners", "year": 2013 }
   */
  override def write(record: InternalRow): Unit = {
    gen.writeAction(FlintWriter.ACTION_CREATE, idOrdinal, record)
    gen.writeLineEnding()
    gen.write(record)
    gen.writeLineEnding()
  }

  override def commit(): WriterCommitMessage = {
    gen.flush()
    null
  }

  override def abort(): Unit = {
    // do nothing.
  }

  override def close(): Unit = {
    gen.close()
  }
}

object FlintPartitionWriter {
  val ID_NAME = "spark.flint.write.id.name"
}
