/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.planner.logical.LogicalPlan;

public class CalciteAnalyzer extends AbstractNodeVisitor<Void, AnalysisContext> {

  public RelBuilder relBuilder;

  public CalciteAnalyzer(RelBuilder relBuilder) {
    this.relBuilder = relBuilder;
  }

  public Void analyze(UnresolvedPlan unresolved, AnalysisContext context) {
    return unresolved.accept(this, context);
  }

  @Override
  public Void visitRelation(Relation node, AnalysisContext context) {
    relBuilder.scan(node.getTableName());
    return null;
  }
}
