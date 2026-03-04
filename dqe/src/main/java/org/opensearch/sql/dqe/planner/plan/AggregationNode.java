/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.plan;

import java.io.IOException;
import java.util.List;
import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Plan node representing an aggregation with group-by keys and aggregate functions. */
@Getter
public class AggregationNode extends DqePlanNode {

  /** Whether this aggregation is a partial (shard-local) or final (coordinator) step. */
  public enum Step {
    PARTIAL,
    FINAL
  }

  private final DqePlanNode child;
  private final List<String> groupByKeys;
  private final List<String> aggregateFunctions;
  private final Step step;

  public AggregationNode(
      DqePlanNode child, List<String> groupByKeys, List<String> aggregateFunctions, Step step) {
    this.child = child;
    this.groupByKeys = groupByKeys;
    this.aggregateFunctions = aggregateFunctions;
    this.step = step;
  }

  /** Deserialize from a stream. */
  public AggregationNode(StreamInput in) throws IOException {
    this.child = DqePlanNode.readPlanNode(in);
    this.groupByKeys = in.readStringList();
    this.aggregateFunctions = in.readStringList();
    this.step = in.readEnum(Step.class);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    DqePlanNode.writePlanNode(out, child);
    out.writeStringCollection(groupByKeys);
    out.writeStringCollection(aggregateFunctions);
    out.writeEnum(step);
  }

  @Override
  public List<DqePlanNode> getChildren() {
    return List.of(child);
  }

  @Override
  public <R, C> R accept(DqePlanVisitor<R, C> visitor, C context) {
    return visitor.visitAggregation(this, context);
  }
}
