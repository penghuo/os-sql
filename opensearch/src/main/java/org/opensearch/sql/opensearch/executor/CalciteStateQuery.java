/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.Bindable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

/**
 * Cached physical plan whose StateStoreScan captures the latest immutable snapshot per execution.
 *
 * <p>The physical tree is compiled to a Bindable once on the first query. Later queries bind the
 * same executable plan to a fresh DataContext. This avoids re-registering the RelNode with a
 * planner and does not parse, analyze, or optimize the PPL query again.
 */
public final class CalciteStateQuery {
  private final RelNode physicalPlan;
  private final StateStore stateStore;
  private volatile CompiledPlan compiledPlan;

  private record CompiledPlan(Bindable<?> executable, Map<String, Object> parameters) {}

  CalciteStateQuery(RelNode physicalPlan, StateStore stateStore) {
    if (!(physicalPlan instanceof EnumerableRel)) {
      throw new IllegalArgumentException("State query physical plan must be enumerable");
    }
    this.physicalPlan = physicalPlan;
    this.stateStore = stateStore;
  }

  public StateStore stateStore() {
    return stateStore;
  }

  public RelNode physicalPlan() {
    return physicalPlan;
  }

  public QueryResponse query(CalcitePlanContext context) throws SQLException {
    CalciteConnection connection = context.connection.unwrap(CalciteConnection.class);
    DataContext baseContext = DataContexts.of(connection, connection.getRootSchema());
    CompiledPlan compiled = compiledPlan();
    DataContext dataContext =
        DataContexts.of(
            name ->
                compiled.parameters().containsKey(name)
                    ? compiled.parameters().get(name)
                    : baseContext.get(name));
    return CalciteResultSetMaterializer.materialize(
        compiled.executable().bind(dataContext),
        physicalPlan.getRowType(),
        context.sysLimit.querySizeLimit());
  }

  private CompiledPlan compiledPlan() {
    CompiledPlan current = compiledPlan;
    if (current == null) {
      synchronized (this) {
        current = compiledPlan;
        if (current == null) {
          Map<String, Object> parameters = new HashMap<>();
          Bindable<?> executable =
              EnumerableInterpretable.toBindable(
                  parameters,
                  CalcitePrepare.Dummy.getSparkHandler(false),
                  (EnumerableRel) physicalPlan,
                  EnumerableRel.Prefer.ARRAY);
          current =
              new CompiledPlan(executable, Collections.unmodifiableMap(new HashMap<>(parameters)));
          compiledPlan = current;
        }
      }
    }
    return current;
  }
}
