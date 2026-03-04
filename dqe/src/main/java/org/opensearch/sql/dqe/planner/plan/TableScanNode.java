/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.plan;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Leaf plan node representing a table scan over an OpenSearch index. */
@Getter
public class TableScanNode extends DqePlanNode {

  private final String indexName;
  private final List<String> columns;

  public TableScanNode(String indexName, List<String> columns) {
    this.indexName = indexName;
    this.columns = columns;
  }

  /** Deserialize from a stream. */
  public TableScanNode(StreamInput in) throws IOException {
    this.indexName = in.readString();
    this.columns = in.readStringList();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(indexName);
    out.writeStringCollection(columns);
  }

  @Override
  public List<DqePlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(DqePlanVisitor<R, C> visitor, C context) {
    return visitor.visitTableScan(this, context);
  }
}
