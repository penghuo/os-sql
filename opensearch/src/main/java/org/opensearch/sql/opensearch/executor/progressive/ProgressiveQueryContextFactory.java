/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progressive;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.executor.Warning;
import org.opensearch.sql.executor.analytics.TimewrapSignals;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;

/** Classifies an optimized physical plan and creates its one progressive execution context. */
public final class ProgressiveQueryContextFactory {

  @FunctionalInterface
  public interface StatementCompiler {
    PreparedStatement compile(RelNode plan) throws SQLException;
  }

  public ProgressiveQueryContextImpl create(RelNode physicalPlan, StatementCompiler compiler)
      throws SQLException {
    TimewrapSignals timewrapSignals = TimewrapSignals.captureAndClear();
    List<Warning> warnings = CalcitePlanContext.drainWarnings();
    CalciteResultSetMaterializer materializer =
        new CalciteResultSetMaterializer(physicalPlan.getRowType(), timewrapSignals, warnings);

    List<CalciteEnumerableIndexScan> aggregationLeaves =
        findNonCompositeAggregationLeaves(physicalPlan);
    if (aggregationLeaves.size() == 1) {
      CalciteEnumerableIndexScan aggregationLeaf = aggregationLeaves.getFirst();
      ReplaceStateStore store = new ReplaceStateStore();
      AggregationResultMapper mapper =
          aggregationLeaf
              .aggregationResultMapper()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "A pushed aggregation leaf must expose its standard result mapper"));
      SearchExecutionObserver observer = new AggregationStatePublisher(store, mapper);
      bindObserver(physicalPlan, observer);
      if (physicalPlan == aggregationLeaf) {
        return new ProgressiveQueryContextImpl(store, materializer, ignored -> {}, null);
      }
      RelNode previewPlan = replaceLeaf(physicalPlan, aggregationLeaf, store);
      PreparedStatement previewStatement = compiler.compile(previewPlan);
      return new ProgressiveQueryContextImpl(store, materializer, ignored -> {}, previewStatement);
    }

    AppendOnlyStateStore store = new AppendOnlyStateStore();
    return new ProgressiveQueryContextImpl(store, materializer, store::append, null);
  }

  private static List<CalciteEnumerableIndexScan> findNonCompositeAggregationLeaves(
      RelNode physicalPlan) {
    List<CalciteEnumerableIndexScan> leaves = new ArrayList<>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof CalciteEnumerableIndexScan scan
            && scan.getPushDownContext().isAggregatePushed()
            && scan.getPushDownContext().getAggSpec() != null
            && !scan.getPushDownContext().getAggSpec().isCompositeAggregation()) {
          leaves.add(scan);
        }
        super.visit(node, ordinal, parent);
      }
    }.go(physicalPlan);
    return leaves;
  }

  private static RelNode bindObserver(
      RelNode physicalPlan, SearchExecutionObserver searchObserver) {
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof CalciteEnumerableIndexScan indexScan) {
          indexScan.bindSearchObserver(searchObserver);
        }
        super.visit(node, ordinal, parent);
      }
    }.go(physicalPlan);
    return physicalPlan;
  }

  private static RelNode replaceLeaf(
      RelNode physicalPlan, CalciteEnumerableIndexScan aggregationLeaf, QueryStateStore store) {
    return physicalPlan.accept(
        new RelShuttleImpl() {
          @Override
          public RelNode visit(TableScan scan) {
            if (scan == aggregationLeaf) {
              RelTraitSet traits = aggregationLeaf.getTraitSet();
              return new StateStoreScan(
                  aggregationLeaf.getCluster(), traits, aggregationLeaf.getRowType(), store);
            }
            return super.visit(scan);
          }
        });
  }
}
