/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark;

import org.apache.spark.annotation.Evolving;

/**
 * Interface mixed into Throwables thrown from Spark.
 *
 * - For backwards compatibility, existing Throwable types can be thrown with an arbitrary error
 *   message with a null error class. See [[SparkException]].
 * - To promote standardization, Throwables should be thrown with an error class and message
 *   parameters to construct an error message with SparkThrowableHelper.getMessage(). New Throwable
 *   types should not accept arbitrary error messages. See [[SparkArithmeticException]].
 *
 * @since 3.2.0
 */
@Evolving
public interface SparkThrowable {
  // Succinct, human-readable, unique, and consistent representation of the error category
  // If null, error class is not set
  String getErrorClass();

  // Portable error identifier across SQL engines
  // If null, error class or SQLSTATE is not set
  String getSqlState();
}
