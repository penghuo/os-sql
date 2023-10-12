/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution;

import lombok.Builder;
import lombok.Data;
import org.opensearch.sql.spark.rest.model.LangType;

@Data
@Builder
public class QueryRequest {
  private final String query;
  private final LangType lang;
}
