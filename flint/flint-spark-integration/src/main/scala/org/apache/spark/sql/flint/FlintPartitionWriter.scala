/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.io.Writer
import java.util.TimeZone

import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

case class FlintPartitionWriter(writer: Writer, dataSchema: StructType)
    extends DataWriter[InternalRow] {

  private lazy val jsonOptions =
    new JSONOptions(CaseInsensitiveMap(Map.empty[String, String]), TimeZone.getDefault.getID, "")

  private lazy val gen = new JacksonGenerator(dataSchema, writer, jsonOptions)

  override def write(record: InternalRow): Unit = {
    gen.write(record)
    gen.writeLineEnding()
  }

  override def commit(): WriterCommitMessage = {
    FlintWriteTaskResult(new TaskCommitMessage("flint success"))
  }

  override def abort(): Unit = {
    // do nothing.
  }

  override def close(): Unit = {
    gen.close()
    writer.close()
  }
}

case class FlintWriteTaskResult(commitMsg: TaskCommitMessage) extends WriterCommitMessage
