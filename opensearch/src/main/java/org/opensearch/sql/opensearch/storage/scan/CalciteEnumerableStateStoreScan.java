/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.Iterator;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.opensearch.executor.StateStore;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;

/** Enumerable leaf that reads one immutable snapshot from a {@link StateStore}. */
public final class CalciteEnumerableStateStoreScan extends AbstractRelNode
    implements EnumerableRel, Scannable {
  private final RelDataType rowType;
  private final StateStore store;

  public CalciteEnumerableStateStoreScan(
      RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType, StateStore store) {
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
      throw new IllegalArgumentException("StateStoreScan cannot have inputs");
    }
    return new CalciteEnumerableStateStoreScan(getCluster(), traitSet, rowType, store);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("updateMode", store.updateMode())
        .item("generation", store.snapshot().generation());
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            OpenSearchRelOptUtil.replaceDot(getCluster().getTypeFactory(), getRowType()),
            pref.preferArray());
    Expression scanOperator = implementor.stash(this, CalciteEnumerableStateStoreScan.class);
    return implementor.result(physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
  }

  @Override
  public Enumerable<@Nullable Object> scan() {
    List<String> fields = getRowType().getFieldNames();
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
        StateStore.Snapshot snapshot = store.snapshot();
        return new StateStoreEnumerator(snapshot.iterator(), fields);
      }
    };
  }

  private static final class StateStoreEnumerator implements Enumerator<Object> {
    private final Iterator<ExprValue> rows;
    private final List<String> fields;
    private ExprValue current;

    private StateStoreEnumerator(Iterator<ExprValue> rows, List<String> fields) {
      this.rows = rows;
      this.fields = fields;
    }

    @Override
    public Object current() {
      if (fields.size() == 1) {
        return resolve(current, fields.getFirst());
      }
      return fields.stream().map(field -> resolve(current, field)).toArray();
    }

    private static Object resolve(ExprValue row, String field) {
      ExprValue exact = row.tupleValue().get(field);
      ExprValue value =
          exact == null ? ExprValueUtils.resolveRefPaths(row, List.of(field.split("\\."))) : exact;
      return value == null || value.isMissing() || value.isNull() ? null : value.valueForCalcite();
    }

    @Override
    public boolean moveNext() {
      if (!rows.hasNext()) {
        current = null;
        return false;
      }
      current = rows.next();
      return true;
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException("State store snapshot enumerator cannot be reset");
    }

    @Override
    public void close() {
      current = null;
    }
  }
}
