/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptionsInRead, JacksonParser}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailureSafeParser}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.opensearch.io.OpenSearchReader

import java.util.TimeZone

private[spark] class OpenSearchRowIterator(
  schema: StructType)
  extends Iterator[InternalRow] {

  lazy val parser = new JacksonParser(schema,
    new JSONOptionsInRead(CaseInsensitiveMap(Map.empty[String, String]), TimeZone.getDefault.getID, ""),
    allowArrayAsStructs = true)
  lazy val stringParser = parser.options.encoding
    .map(enc => CreateJacksonParser.string(_: JsonFactory, _: String))
    .getOrElse(CreateJacksonParser.string(_: JsonFactory, _: String))
  lazy val safeParser = new FailureSafeParser[String](
    input => parser.parse(input, stringParser, UTF8String.fromString),
    parser.options.parseMode,
    schema,
    parser.options.columnNameOfCorruptRecord)

  lazy val openSearchReader = {
    val reader = new OpenSearchReader()
    reader.open()
    reader
  }

  var rows: Iterator[InternalRow] = Iterator.empty

  /**
   * Todo. consider multiple-line json.
   * @return
   */
  override def hasNext: Boolean = {
    if (rows.hasNext) {
      true
    } else if (openSearchReader.hasNext) {
      rows = safeParser.parse(openSearchReader.next())
      rows.hasNext
    } else {
      false
    }
  }

  override def next(): InternalRow = {
    rows.next()
  }
}
