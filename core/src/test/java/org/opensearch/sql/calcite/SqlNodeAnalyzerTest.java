/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalciteSqlValidator;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.executor.OpenSearchTypeSystem;
import org.opensearch.sql.executor.QueryType;

class SqlNodeAnalyzerTest extends AnalyzerTestBase {

  private final SqlNodeAnalyzer analyzer = new SqlNodeAnalyzer();

  private SqlNode analyze(Function function) {
    return analyzer.analyze(function, null);
  }

  @Test
  void visitFunctionWithIdentifierArgument() {
    Function function = new Function("lower", List.of(new QualifiedName("name")));

    SqlNode result = analyze(function);
    assertInstanceOf(SqlBasicCall.class, result);

    SqlBasicCall call = (SqlBasicCall) result;
    assertEquals("LOWER", call.getOperator().getName());
    assertEquals(1, call.getOperandList().size());
    assertInstanceOf(SqlIdentifier.class, call.operand(0));

    SqlIdentifier identifier = (SqlIdentifier) call.operand(0);
    assertEquals(List.of("name"), identifier.names);
  }

  @Test
  void visitFunctionWithLiteralArgument() {
    Function function = new Function("abs", List.of(new Literal(-5, DataType.INTEGER)));

    SqlNode result = analyze(function);
    assertInstanceOf(SqlBasicCall.class, result);

    SqlBasicCall call = (SqlBasicCall) result;
    assertEquals("ABS", call.getOperator().getName());
    assertEquals(1, call.getOperandList().size());
    assertInstanceOf(SqlLiteral.class, call.operand(0));

    SqlLiteral literal = (SqlLiteral) call.operand(0);
    assertEquals(BigDecimal.valueOf(-5), literal.getValueAs(BigDecimal.class));
  }

  @Test
  void visitFunctionWithMultipleArguments() {
    Function function =
        new Function(
            "concat",
            List.of(
                new Literal("a", DataType.STRING),
                new Literal("b", DataType.STRING),
                new Literal("c", DataType.STRING)));

    SqlNode result = analyze(function);
    assertInstanceOf(SqlBasicCall.class, result);

    SqlBasicCall call = (SqlBasicCall) result;
    assertEquals("CONCAT", call.getOperator().getName());
    assertEquals(3, call.getOperandList().size());
    assertEquals("a", ((SqlLiteral) call.operand(0)).getValueAs(String.class));
    assertEquals("b", ((SqlLiteral) call.operand(1)).getValueAs(String.class));
    assertEquals("c", ((SqlLiteral) call.operand(2)).getValueAs(String.class));
  }

  @Test
  void visitFunctionWithNestedCall() {
    Function inner =
        new Function(
            "concat",
            List.of(new Literal("x", DataType.STRING), new Literal("y", DataType.STRING)));
    Function outer = new Function("lower", List.of(inner));

    SqlNode result = analyze(outer);
    assertInstanceOf(SqlBasicCall.class, result);

    SqlBasicCall outerCall = (SqlBasicCall) result;
    assertEquals("LOWER", outerCall.getOperator().getName());
    assertEquals(1, outerCall.getOperandList().size());
    assertInstanceOf(SqlBasicCall.class, outerCall.operand(0));

    SqlBasicCall innerCall = (SqlBasicCall) outerCall.operand(0);
    assertEquals("CONCAT", innerCall.getOperator().getName());
    assertEquals(2, innerCall.getOperandList().size());
  }

  @Test
  void visitFunctionWithQualifiedName() {
    Function function =
        new Function("lower", List.of(QualifiedName.of("catalog", "schema", "table", "field")));

    SqlNode result = analyze(function);
    assertInstanceOf(SqlBasicCall.class, result);

    SqlBasicCall call = (SqlBasicCall) result;
    assertEquals("LOWER", call.getOperator().getName());
    assertInstanceOf(SqlIdentifier.class, call.operand(0));

    SqlIdentifier identifier = (SqlIdentifier) call.operand(0);
    assertEquals(List.of("catalog", "schema", "table", "field"), identifier.names);
  }

  @Test
  void visitFunctionWithUnknownOperatorThrowsException() {
    Function function = new Function("unknown_function", List.of());

    assertThrows(IllegalArgumentException.class, () -> analyze(function));
  }

  @Test
  void testResolve() {
    SqlValidator sqlValidator = createSqlValidator();
    Function function = new Function("abs", List.of(new Literal("5", DataType.STRING)));
    SqlNode sqlCall = analyze(function);
    SqlNode converted = sqlValidator.validate(sqlCall);
    ExtendedRexBuilder rexBuilder = calcitePlanContext().rexBuilder;

    System.out.println(converted.toString());
    System.out.println("RESULT");
  }

  // FROM CalcitePrepareImpl
  private static SqlValidator createSqlValidator() {
    final SqlOperatorTable opTab0 =
        CalciteConnectionConfigImpl.DEFAULT.fun(
            SqlOperatorTable.class, SqlStdOperatorTable.instance());
    final List<SqlOperatorTable> list = new ArrayList<>();
    list.add(opTab0);
    final SqlOperatorTable opTab = SqlOperatorTables.chain(list);

    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            rootSchema,
            ImmutableList.of(),
            new JavaTypeFactoryImpl(),
            CalciteConnectionConfigImpl.DEFAULT);
    return new CalciteSqlValidator(opTab, catalogReader, TYPE_FACTORY, SqlValidator.Config.DEFAULT);
  }

  private CalcitePlanContext calcitePlanContext() {
    return CalcitePlanContext.create(buildFrameworkConfig(), SysLimit.DEFAULT, QueryType.PPL);
  }

  private FrameworkConfig buildFrameworkConfig() {
    // Use simple calcite schema since we don't compute tables in advance of the query.
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
    final SchemaPlus opensearchSchema =
        rootSchema.add(
            OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME,
            new OpenSearchSchema(new DefaultDataSourceService()));
    Frameworks.ConfigBuilder configBuilder =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT) // TODO check
            .defaultSchema(opensearchSchema)
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.standard())
            .typeSystem(OpenSearchTypeSystem.INSTANCE);
    return configBuilder.build();
  }
}
