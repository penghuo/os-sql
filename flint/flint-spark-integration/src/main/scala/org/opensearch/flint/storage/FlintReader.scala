/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.storage

trait FlintReader {

  def hasNext(): Boolean

  def next(): String

  def close(): Unit
}
