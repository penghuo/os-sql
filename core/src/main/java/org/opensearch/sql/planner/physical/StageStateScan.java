/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.stage.StageId;
import org.opensearch.sql.planner.stage.StageState;
import org.opensearch.sql.planner.stage.StageStateTable;

@ToString
@EqualsAndHashCode(callSuper = false, of = "values")
public class StageStateScan extends PhysicalPlan {
  private final StageId stageId;

  private final StageStateTable stageStateTable;

  private Iterator<StageState> stageStateIterator;

  public StageStateScan(StageId stageId, StageStateTable stageStateTable) {
    this.stageId = stageId;
    this.stageStateTable = stageStateTable;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitStageState(this, context);
  }

  @Override
  public void open() {
    if (stageStateTable.getStageStage(stageId).isPresent()) {
      stageStateIterator = Iterators.singletonIterator(stageStateTable.getStageStage(stageId).get());
    } else {
      stageStateIterator = Collections.emptyIterator();
    }
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return ImmutableList.of();
  }

  @Override
  public boolean hasNext() {
    return stageStateIterator.hasNext();
  }

  @Override
  public ExprValue next() {
    StageState next = stageStateIterator.next();
    Map<String, ExprValue> map = new HashMap<>();
    map.put("status", integerValue(next.getStatus()));
    map.put("size", integerValue(next.getRowCount()));
    ExprTupleValue value = ExprTupleValue.fromExprValueMap(map);
    return value;
  }
}
