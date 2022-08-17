/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.stage;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.splits.SplitManager;
import org.opensearch.sql.storage.Table;

public class StageStateTable implements Table {
  public static StageStateTable INSTANCE = new StageStateTable();

  private Map<StageId, StageState> stageStateMap = new HashMap<>();

  public Optional<StageState> getStageStage(StageId stageId) {
    return Optional.ofNullable(stageStateMap.get(stageId));
  }

  public void updateStageState(StageId stageId, StageState stageState) {
    stageStateMap.put(stageId, stageState);
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return null;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new DefaultImplementor<Void>(), null);
  }

  @Override
  public SplitManager getSplitManager() {
    return new SplitManager() {
      @Override
      public CompletableFuture<List<Split>> nextBatch() {
        return CompletableFuture.completedFuture(Collections.singletonList(new StageStateSplit()));
      }

      @Override
      public void close() {

      }
    };
  }

  static class StageStateSplit implements Split {

  }
}
