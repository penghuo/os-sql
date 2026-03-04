/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.metadata;

import io.trino.spi.type.Type;
import java.util.List;

public record TableInfo(String indexName, List<ColumnInfo> columns) {
  public record ColumnInfo(String name, String openSearchType, Type trinoType) {}
}
