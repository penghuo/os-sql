/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.sql.parser;

import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;

@RequiredArgsConstructor
public class AstStatementBuilder extends OpenSearchSQLParserBaseVisitor<Statement> {

  private final AstBuilder astBuilder;

  private final StatementBuilderContext context;

  @Override
  public Statement visitSqlStatement(OpenSearchSQLParser.SqlStatementContext ctx) {
    Query query = new Query(addQuerySizeLimit(astBuilder.visit(ctx)), context.fetchSize);
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

  /**
   * Enforce query size limit.
   *
   * @param plan
   * @return
   */
  private UnresolvedPlan addQuerySizeLimit(UnresolvedPlan plan) {
    // Ignore if it is pagination query.
    if (context.fetchSize > 0) {
      return plan;
    }
    Limit limit = new Limit(context.getQuerySizeLimit(), 0);
    limit.attach(((List<UnresolvedPlan>) plan.getChild()).get(0));;
    plan.attach(limit);
    return plan;
  }
}
