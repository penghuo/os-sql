/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.sql.calcite;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Relational expression representing a scan of an OpenSearch type.
 *
 * <p> Additional operations might be applied,
 * using the "find" method.
 */
public class OpenSearchTableScan extends TableScan implements OpenSearchRel {
  private final OpenSearchTable openSearchTable;
  private final @Nullable RelDataType projectRowType;

  /**
   * Creates an OpenSearchTableScan.
   *
   * @param cluster Cluster
   * @param traitSet Trait set
   * @param table Table
   * @param openSearchTable OpenSearch table
   * @param projectRowType Fields and types to project; null to project raw row
   */
  OpenSearchTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                      RelOptTable table, OpenSearchTable openSearchTable,
                      @Nullable RelDataType projectRowType) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.openSearchTable =
        requireNonNull(openSearchTable, "OpenSearchTable");
    this.projectRowType = projectRowType;

    assert getConvention() == OpenSearchRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    final float f = projectRowType == null ? 1f : (float) projectRowType.getFieldCount() / 100f;
    final RelOptCost cost = super.computeSelfCost(planner, mq);
    return requireNonNull(cost, "cost").multiplyBy(.1 * f);
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(OpenSearchToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : OpenSearchRules.RULES) {
      planner.addRule(rule);
    }

    // remove this rule otherwise elastic can't correctly interpret approx_count_distinct()
    // it is converted to cardinality aggregation in Elastic
    planner.removeRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
  }

  @Override public void implement(Implementor implementor) {
    implementor.openSearchTable = openSearchTable;
    implementor.table = table;
  }
}
