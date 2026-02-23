/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.sort;

import static org.junit.jupiter.api.Assertions.*;

import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.SortItem;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.DqeAnalysisException;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.type.ExpressionTypeChecker;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("OrderByAnalyzer")
class OrderByAnalyzerTest {

  private OrderByAnalyzer analyzer;
  private Scope scope;

  @BeforeEach
  void setUp() {
    analyzer = new OrderByAnalyzer(new ExpressionTypeChecker());
    DqeTableHandle table =
        new DqeTableHandle("test_table", null, List.of("test_table"), 1L, null);
    DqeColumnHandle nameCol =
        new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, "name.keyword", false);
    DqeColumnHandle ageCol =
        new DqeColumnHandle("age", "age", DqeTypes.INTEGER, true, null, false);
    DqeColumnHandle unsortableCol =
        new DqeColumnHandle("description", "description", DqeTypes.VARCHAR, false, null, false);
    scope = new Scope(table, List.of(nameCol, ageCol, unsortableCol), Optional.empty());
  }

  @Test
  @DisplayName("ORDER BY sortable column succeeds")
  void orderBySortableColumn() {
    SortItem item =
        new SortItem(
            new Identifier("age"),
            SortItem.Ordering.ASCENDING,
            SortItem.NullOrdering.UNDEFINED);
    List<SortSpecification> result = analyzer.analyze(List.of(item), scope);
    assertEquals(1, result.size());
    assertEquals("age", result.get(0).getColumn().getFieldName());
    assertEquals(SortSpecification.SortDirection.ASC, result.get(0).getDirection());
    assertEquals(SortSpecification.NullOrdering.NULLS_LAST, result.get(0).getNullOrdering());
  }

  @Test
  @DisplayName("ORDER BY DESC with NULLS FIRST")
  void orderByDesc() {
    SortItem item =
        new SortItem(
            new Identifier("age"),
            SortItem.Ordering.DESCENDING,
            SortItem.NullOrdering.UNDEFINED);
    List<SortSpecification> result = analyzer.analyze(List.of(item), scope);
    assertEquals(SortSpecification.SortDirection.DESC, result.get(0).getDirection());
    assertEquals(SortSpecification.NullOrdering.NULLS_FIRST, result.get(0).getNullOrdering());
  }

  @Test
  @DisplayName("ORDER BY with explicit NULLS LAST")
  void orderByExplicitNullsLast() {
    SortItem item =
        new SortItem(
            new Identifier("age"),
            SortItem.Ordering.DESCENDING,
            SortItem.NullOrdering.LAST);
    List<SortSpecification> result = analyzer.analyze(List.of(item), scope);
    assertEquals(SortSpecification.NullOrdering.NULLS_LAST, result.get(0).getNullOrdering());
  }

  @Test
  @DisplayName("ORDER BY non-sortable column throws")
  void orderByNonSortableThrows() {
    SortItem item =
        new SortItem(
            new Identifier("description"),
            SortItem.Ordering.ASCENDING,
            SortItem.NullOrdering.UNDEFINED);
    DqeAnalysisException ex =
        assertThrows(DqeAnalysisException.class, () -> analyzer.analyze(List.of(item), scope));
    assertTrue(ex.getMessage().contains("not sortable"));
  }

  @Test
  @DisplayName("ORDER BY unknown column throws")
  void orderByUnknownColumnThrows() {
    SortItem item =
        new SortItem(
            new Identifier("unknown"),
            SortItem.Ordering.ASCENDING,
            SortItem.NullOrdering.UNDEFINED);
    assertThrows(DqeAnalysisException.class, () -> analyzer.analyze(List.of(item), scope));
  }

  @Test
  @DisplayName("Multiple ORDER BY columns")
  void multipleOrderByColumns() {
    SortItem item1 =
        new SortItem(
            new Identifier("name"),
            SortItem.Ordering.ASCENDING,
            SortItem.NullOrdering.UNDEFINED);
    SortItem item2 =
        new SortItem(
            new Identifier("age"),
            SortItem.Ordering.DESCENDING,
            SortItem.NullOrdering.UNDEFINED);
    List<SortSpecification> result = analyzer.analyze(List.of(item1, item2), scope);
    assertEquals(2, result.size());
    assertEquals("name", result.get(0).getColumn().getFieldName());
    assertEquals("age", result.get(1).getColumn().getFieldName());
  }
}
