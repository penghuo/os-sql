/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution;

import lombok.Data;

@Data
public class QueryRequest {
  private final String query;
  private final String lang;
}
