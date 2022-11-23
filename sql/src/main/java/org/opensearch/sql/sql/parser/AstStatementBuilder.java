/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.sql.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;

import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.statement.ddl.Column;
import org.opensearch.sql.ast.statement.ddl.CreateTable;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;

@RequiredArgsConstructor
public class AstStatementBuilder extends OpenSearchSQLParserBaseVisitor<Statement> {

  private final AstBuilder astBuilder;

  private final StatementBuilderContext context;

  @Override
  public Statement visitAdminStatement(OpenSearchSQLParser.AdminStatementContext ctx) {
    Query query = new Query(astBuilder.visit(ctx));
    return context.isExplain ? new Explain(query) : query;
  }

  @Override
  public Statement visitDmlStatement(OpenSearchSQLParser.DmlStatementContext ctx) {
    Query query = new Query(astBuilder.visit(ctx));
    return context.isExplain ? new Explain(query) : query;
  }

  @Override
  public Statement visitCreateTable(OpenSearchSQLParser.CreateTableContext ctx) {
    QualifiedName tableName = qualifiedName(ctx.tableName().getText());
    List<Column> columns = ctx.createDefinitions()
        .createDefinition()
        .stream()
        .map(def -> new Column(def.columnName().getText(), def.dataType().getText()))
        .collect(Collectors.toList());
    String fileFormat = StringUtils.unquoteText(ctx.fileFormat.getText());
    String location = StringUtils.unquoteText(ctx.location.getText());

    return new CreateTable(tableName, columns, fileFormat, location);
  }

  @Override
  protected Statement aggregateResult(Statement aggregate, Statement nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }

  @Data
  @Builder
  public static class StatementBuilderContext {
    private final boolean isExplain;
  }
}
