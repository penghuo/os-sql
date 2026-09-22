/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progressive;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;

/** Enumerable leaf that exposes the current rows of a {@link QueryStateStore} to Calcite. */
public final class StateStoreScan extends AbstractRelNode implements EnumerableRel {
  private final RelDataType rowType;
  private final QueryStateStore store;

  StateStoreScan(
      RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType, QueryStateStore store) {
    super(cluster, traitSet);
    this.rowType = rowType;
    this.store = store;
  }

  @Override
  protected RelDataType deriveRowType() {
    return rowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    if (!inputs.isEmpty()) {
      throw new IllegalArgumentException("StateStoreScan is a leaf");
    }
    return new StateStoreScan(getCluster(), traitSet, rowType, store);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            OpenSearchRelOptUtil.replaceDot(getCluster().getTypeFactory(), rowType),
            pref.preferArray());
    Expression scan = implementor.stash(this, StateStoreScan.class);
    return implementor.result(physType, Blocks.toBlock(Expressions.call(scan, "scan")));
  }

  public Enumerable<Object> scan() {
    List<String> fields = rowType.getFieldNames();
    List<ExprValue> currentRows = store.rows();
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
        return Linq4j.enumerator(
            currentRows.stream().map(row -> toCalciteRow(row, fields)).toList());
      }
    };
  }

  private static Object toCalciteRow(ExprValue row, List<String> fields) {
    if (fields.size() == 1) {
      return resolve(row, fields.getFirst());
    }
    return fields.stream().map(field -> resolve(row, field)).toArray();
  }

  private static Object resolve(ExprValue row, String field) {
    ExprValue value = ExprValueUtils.resolveRefPaths(row, List.of(field.split("\\.")));
    return value == null ? null : value.valueForCalcite();
  }
}
