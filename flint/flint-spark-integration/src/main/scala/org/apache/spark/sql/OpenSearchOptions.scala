/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.util.Locale

class OpenSearchOptions(
  val parameters: CaseInsensitiveMap[String])
  extends Serializable with Logging {

  import OpenSearchOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val pushDownPredicate = parameters.getOrElse(OS_PUSH_DOWN, "false").toBoolean

}

object OpenSearchOptions {
  private val curId = new java.util.concurrent.atomic.AtomicLong(0L)
  private val optionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    optionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val OS_URL = newOption("url")
  val OS_PUSH_DOWN = newOption("pushDown")
}
