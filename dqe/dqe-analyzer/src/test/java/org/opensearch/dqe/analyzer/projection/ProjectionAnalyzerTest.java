/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.projection;

import static org.junit.jupiter.api.Assertions.*;

import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.StringLiteral;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.type.TypedExpression;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("ProjectionAnalyzer")
class ProjectionAnalyzerTest {

  private ProjectionAnalyzer projectionAnalyzer;
  private Scope scope;
  private DqeColumnHandle nameCol;
  private DqeColumnHandle ageCol;
  private DqeColumnHandle salaryCol;

  @BeforeEach
  void setUp() {
    projectionAnalyzer = new ProjectionAnalyzer();
    DqeTableHandle table = new DqeTableHandle("test_table", null, List.of("test_table"), 1L, null);
    nameCol = new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, "name.keyword", false);
    ageCol = new DqeColumnHandle("age", "age", DqeTypes.INTEGER, true, null, false);
    salaryCol = new DqeColumnHandle("salary", "salary", DqeTypes.DOUBLE, true, null, false);
    scope = new Scope(table, List.of(nameCol, ageCol, salaryCol), Optional.empty());
  }

  @Test
  @DisplayName("Single column reference requires only that column")
  void singleColumnReference() {
    List<TypedExpression> select =
        List.of(new TypedExpression(new Identifier("name"), DqeTypes.VARCHAR));
    RequiredColumns required =
        projectionAnalyzer.computeRequiredColumns(select, null, List.of(), scope);
    assertEquals(1, required.size());
    assertTrue(required.getColumns().contains(nameCol));
  }

  @Test
  @DisplayName("Multiple column references")
  void multipleColumnReferences() {
    List<TypedExpression> select =
        List.of(
            new TypedExpression(new Identifier("name"), DqeTypes.VARCHAR),
            new TypedExpression(new Identifier("age"), DqeTypes.INTEGER));
    RequiredColumns required =
        projectionAnalyzer.computeRequiredColumns(select, null, List.of(), scope);
    assertEquals(2, required.size());
    assertTrue(required.getColumns().contains(nameCol));
    assertTrue(required.getColumns().contains(ageCol));
  }

  @Test
  @DisplayName("Arithmetic expression extracts both column references")
  void arithmeticExpressionExtractsColumns() {
    ArithmeticBinaryExpression arith =
        new ArithmeticBinaryExpression(
            ArithmeticBinaryExpression.Operator.MULTIPLY,
            new Identifier("salary"),
            new Identifier("age"));
    List<TypedExpression> select = List.of(new TypedExpression(arith, DqeTypes.DOUBLE));
    RequiredColumns required =
        projectionAnalyzer.computeRequiredColumns(select, null, List.of(), scope);
    assertEquals(2, required.size());
    assertTrue(required.getColumns().contains(salaryCol));
    assertTrue(required.getColumns().contains(ageCol));
  }

  @Test
  @DisplayName("WHERE clause columns are included")
  void whereClauseColumnsIncluded() {
    List<TypedExpression> select =
        List.of(new TypedExpression(new Identifier("name"), DqeTypes.VARCHAR));
    TypedExpression where = new TypedExpression(new Identifier("age"), DqeTypes.INTEGER);
    RequiredColumns required =
        projectionAnalyzer.computeRequiredColumns(select, where, List.of(), scope);
    assertEquals(2, required.size());
    assertTrue(required.getColumns().contains(nameCol));
    assertTrue(required.getColumns().contains(ageCol));
  }

  @Test
  @DisplayName("ORDER BY columns are included")
  void orderByColumnsIncluded() {
    List<TypedExpression> select =
        List.of(new TypedExpression(new Identifier("name"), DqeTypes.VARCHAR));
    List<TypedExpression> orderBy =
        List.of(new TypedExpression(new Identifier("salary"), DqeTypes.DOUBLE));
    RequiredColumns required =
        projectionAnalyzer.computeRequiredColumns(select, null, orderBy, scope);
    assertEquals(2, required.size());
    assertTrue(required.getColumns().contains(nameCol));
    assertTrue(required.getColumns().contains(salaryCol));
  }

  @Test
  @DisplayName("Literal-only SELECT requires no columns from scan")
  void literalOnlySelectRequiresNoColumns() {
    List<TypedExpression> select =
        List.of(new TypedExpression(new StringLiteral("hello"), DqeTypes.VARCHAR));
    RequiredColumns required =
        projectionAnalyzer.computeRequiredColumns(select, null, List.of(), scope);
    assertEquals(0, required.size());
  }

  @Test
  @DisplayName("Duplicate column references result in single required column")
  void deduplicatesColumns() {
    List<TypedExpression> select =
        List.of(
            new TypedExpression(new Identifier("name"), DqeTypes.VARCHAR),
            new TypedExpression(new Identifier("name"), DqeTypes.VARCHAR));
    RequiredColumns required =
        projectionAnalyzer.computeRequiredColumns(select, null, List.of(), scope);
    assertEquals(1, required.size());
  }

  @Test
  @DisplayName("allColumns creates RequiredColumns with isAllColumns true")
  void allColumnsFactory() {
    RequiredColumns all = RequiredColumns.allColumns(List.of(nameCol, ageCol, salaryCol));
    assertTrue(all.isAllColumns());
    assertEquals(3, all.size());
  }

  @Test
  @DisplayName("getFieldNames returns field paths")
  void getFieldNames() {
    RequiredColumns required = RequiredColumns.allColumns(List.of(nameCol));
    String[] fieldNames = required.getFieldNames();
    assertEquals(1, fieldNames.length);
    assertEquals("name", fieldNames[0]);
  }
}
