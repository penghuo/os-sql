/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Plan node representing a table scan against a Lucene-backed OpenSearch index. This is a leaf node
 * with no children. In distributed execution, this node runs on shard-holding data nodes and reads
 * directly from Lucene segments via DocValues.
 */
public class LuceneTableScanNode extends PlanNode {

  private final String indexName;
  private final RelDataType outputType;
  private final List<String> projectedColumns;

  public LuceneTableScanNode(
      PlanNodeId id, String indexName, RelDataType outputType, List<String> projectedColumns) {
    super(id);
    this.indexName = indexName;
    this.outputType = outputType;
    this.projectedColumns = List.copyOf(projectedColumns);
  }

  public String getIndexName() {
    return indexName;
  }

  public RelDataType getOutputType() {
    return outputType;
  }

  public List<String> getProjectedColumns() {
    return projectedColumns;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of();
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitLuceneTableScan(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (!newChildren.isEmpty()) {
      throw new IllegalArgumentException("LuceneTableScanNode is a leaf node with no children");
    }
    return this;
  }

  @Override
  public String toString() {
    return "LuceneTableScanNode{id="
        + getId()
        + ", index="
        + indexName
        + ", columns="
        + projectedColumns
        + "}";
  }
}
