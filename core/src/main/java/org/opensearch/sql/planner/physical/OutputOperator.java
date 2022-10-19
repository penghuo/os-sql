/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.planner.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class OutputOperator extends PhysicalPlan {

  private final PhysicalPlan plan;

  private final Consumer<ExecutionEngine.QueryResponse> listener;

  @Override
  public void open() {
    try {
      List<ExprValue> result = new ArrayList<>();
      plan.open();

      while (plan.hasNext()) {
        result.add(plan.next());
      }

      ExecutionEngine.QueryResponse response =
          new ExecutionEngine.QueryResponse(plan.schema(), result);
      listener.accept(response);
    } finally {
      plan.close();
    }
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public ExprValue next() {
    return ExprMissingValue.of();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(plan);
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitOutput(this, context);
  }
}
