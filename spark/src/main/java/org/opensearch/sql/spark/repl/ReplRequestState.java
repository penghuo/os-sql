/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.repl;

public enum ReplRequestState {
  PENDING,
  RUNNING,
  SUCCESS,
  FAILED,
}
