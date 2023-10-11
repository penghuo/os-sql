/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution;

public enum ReplSessionState {
  STANDBY,
  STARTING,
  RUNNING,
  FAILED
}
