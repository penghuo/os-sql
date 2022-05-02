/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.memory;

public class TooLargePageException extends RuntimeException {
  TooLargePageException(long size) {
    super("Cannot allocate a page of " + size + " bytes.");
  }
}
