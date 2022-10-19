/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.task.TaskId;
import org.opensearch.sql.opensearch.executor.transport.dataplane.QLDataWriter;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

@RequiredArgsConstructor
public class RemoteSinkOperator extends PhysicalPlan {

  private static final Logger LOG = LogManager.getLogger();

  private final QLDataWriter dataWriter;

  @Getter
  private final PhysicalPlan input;

  private final TaskId taskId;

  private List<ExprValue> values = new ArrayList<>();

  public RemoteSinkOperator(PhysicalPlan input, QLDataWriter dataWriter, TaskId taskId) {
    this.input = input;
    this.dataWriter = dataWriter;
    this.taskId = taskId;
  }

  @Override
  public void open() {
    input.open();
    while (input.hasNext()) {
      values.add(input.next());
    }
    LOG.info("open");
  }

  @Override
  public boolean hasNext() {
    LOG.info("hasNext {}", !values.isEmpty());
    return !values.isEmpty();
  }

  @SneakyThrows
  @Override
  public ExprValue next() {
    LOG.info("next");
    dataWriter.write(taskId, values);
    values.clear();
    return ExprMissingValue.of();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }
}
