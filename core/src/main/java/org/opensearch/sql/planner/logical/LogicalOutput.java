/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.function.Consumer;
import lombok.Getter;
import org.opensearch.sql.executor.ExecutionEngine;

public class LogicalOutput extends LogicalPlan {

  @Getter
  private final Consumer<ExecutionEngine.QueryResponse> consumer;

  public LogicalOutput(
      LogicalPlan child,
      Consumer<ExecutionEngine.QueryResponse> consumer) {
    super(Collections.singletonList(child));
    this.consumer = consumer;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitOutput(this, context);
  }
}
