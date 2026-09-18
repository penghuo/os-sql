/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableStateStoreScan;

/** Creates a cached state query for supported single-source physical plans. */
final class CalciteStateQueryFactory {
  enum Producer {
    ROOT_APPEND,
    AGGREGATION_REPLACE
  }

  record Created(CalciteStateQuery query, Producer producer) {}

  private CalciteStateQueryFactory() {}

  static Created create(RelNode physicalPlan) {
    CalciteEnumerableIndexScan leaf = findSupportedLeaf(physicalPlan);
    if (leaf == null) {
      return null;
    }
    var aggSpec = leaf.getPushDownContext().getAggSpec();
    if (aggSpec == null || aggSpec.isCompositeAggregation()) {
      StateStore store = new StateStore(StateStore.UpdateMode.APPEND);
      RelNode identity =
          new CalciteEnumerableStateStoreScan(
              physicalPlan.getCluster(),
              physicalPlan.getTraitSet(),
              physicalPlan.getRowType(),
              store);
      return new Created(new CalciteStateQuery(identity, store), Producer.ROOT_APPEND);
    }

    StateStore store = new StateStore(StateStore.UpdateMode.REPLACE);
    RelNode rewritten =
        physicalPlan.accept(
            new RelShuttleImpl() {
              @Override
              public RelNode visit(TableScan scan) {
                if (scan == leaf) {
                  return new CalciteEnumerableStateStoreScan(
                      scan.getCluster(), scan.getTraitSet(), scan.getRowType(), store);
                }
                return super.visit(scan);
              }
            });
    return new Created(new CalciteStateQuery(rewritten, store), Producer.AGGREGATION_REPLACE);
  }

  private static CalciteEnumerableIndexScan findSupportedLeaf(RelNode rel) {
    if (rel instanceof CalciteEnumerableIndexScan scan) {
      return scan;
    }
    if (!(rel instanceof SingleRel) || !isRowLocal(rel)) {
      return null;
    }
    return findSupportedLeaf(rel.getInput(0));
  }

  private static boolean isRowLocal(RelNode rel) {
    return rel instanceof Project
        || rel instanceof Filter
        || rel instanceof Calc
        || rel instanceof LogicalSystemLimit
        || (rel instanceof Sort sort && sort.getCollation().getFieldCollations().isEmpty());
  }
}
