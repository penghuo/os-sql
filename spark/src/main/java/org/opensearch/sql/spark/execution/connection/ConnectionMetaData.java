/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.connection;

import lombok.Data;

@Data
public class ConnectionMetaData {
  private final String applicationID;
  private final String jobId;
  private final ConnectionState connectionState;
  private final Long lastUpdateTime;
}
