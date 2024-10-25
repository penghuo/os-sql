/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ppl.parser;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;

/** Build {@link Statement} from PPL Query. */
@RequiredArgsConstructor
public class AstStatementBuilder extends OpenSearchPPLParserBaseVisitor<Statement> {

  private final AstBuilder astBuilder;

  private final StatementBuilderContext context;

  @Override
  public Statement visitDmlStatement(OpenSearchPPLParser.DmlStatementContext ctx) {
    Query query = new Query(addQuerySizeLimit(addSelectAll(astBuilder.visit(ctx))),
        context.getFetchSize());
    return context.isExplain ? new Explain(query) : query;
  }

  @Override
  protected Statement aggregateResult(Statement aggregate, Statement nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }

  @Data
  @Builder
  public static class StatementBuilderContext {
    private final boolean isExplain;
    private final int fetchSize;
    private final int querySizeLimit;
  }

  private UnresolvedPlan addSelectAll(UnresolvedPlan plan) {
    if ((plan instanceof Project) && !((Project) plan).isExcluded()) {
      return plan;
    } else {
      return new Project(ImmutableList.of(AllFields.of())).attach(plan);
    }
  }

  /**
   * FIXME, enforce query size limit.
   *
   * @param plan
   * @return
   */
  private UnresolvedPlan addQuerySizeLimit(UnresolvedPlan plan) {
    Limit limit = new Limit(context.getQuerySizeLimit(), 0);
    limit.attach(((List<UnresolvedPlan>) plan.getChild()).get(0));;
    plan.attach(limit);
    return plan;
  }
}
