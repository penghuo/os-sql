/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.planner.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.opensearch.sql.executor.task.TaskId;

public class LogicalRemote extends LogicalPlan {

  @Getter
  private final RemoteType type;

  private List<TaskId> taskIdList;

  public LogicalRemote(
      RemoteType type) {
    super(Collections.emptyList());
    this.type = type;
    this.taskIdList = new ArrayList<>();
  }
  public LogicalRemote(
      LogicalPlan child,
      RemoteType type) {
    super(Collections.singletonList(child));
    this.type = type;
    this.taskIdList = new ArrayList<>();
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRemote(this, context);
  }

  public enum RemoteType {
    SOURCE,
    SINK;
  }
}
