/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.storage

class FlintOptions(options: Map[String, String]) extends Serializable {
  import FlintOptions._

  def getIndexName: String =
    options.getOrElse(
      "path",
      options.getOrElse(INDEX_NAME, throw new NoSuchElementException("index or path not found")))

  def getHost: String = options.getOrElse(HOST, "localhost")

  def getPort: Int = options.getOrElse(PORT, "9200").toInt

  def arrayFields: Set[String] =
    options
      .getOrElse(ARRAY_FIELDS, "")
      .split(",")
      .map(_.trim)
      .toSet
}

object FlintOptions {
  val INDEX_NAME: String = "index"
  val HOST: String = "host"
  val PORT: String = "port"
  val ARRAY_FIELDS: String = "array_fields"
}
