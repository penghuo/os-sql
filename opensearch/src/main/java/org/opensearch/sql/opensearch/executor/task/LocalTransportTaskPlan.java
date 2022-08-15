/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import java.util.List;
import java.util.function.Consumer;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.logical.LogicalPlan;

public class LocalTransportTaskPlan extends TransportTaskPlan {

  @Getter
  private final Consumer<List<ExprValue>> consumer;

  public LocalTransportTaskPlan(LogicalPlan logicalPlan,
                                TaskNode node,
                                Consumer<List<ExprValue>> consumer) {
    super(logicalPlan, node);
    this.consumer = consumer;
  }
}
