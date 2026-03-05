/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.trino.sql.tree.Statement;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.security.SecurityContext;
import org.opensearch.dqe.analyzer.sort.OperatorSelectionRule;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeMetadata;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.metadata.DqeTableNotFoundException;
import org.opensearch.dqe.parser.DqeAccessDeniedException;
import org.opensearch.dqe.parser.DqeSqlParser;
import org.opensearch.dqe.parser.DqeUnsupportedOperationException;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("DqeAnalyzer")
class DqeAnalyzerTest {

  private DqeAnalyzer analyzer;
  private DqeSqlParser parser;
  private DqeMetadata metadata;
  private SecurityContext allowAllSecurity;
  private SecurityContext denyAllSecurity;

  private static final DqeTableHandle EMPLOYEES_TABLE =
      new DqeTableHandle("employees", null, List.of("employees"), 1L, null);
  private static final DqeTableHandle T_TABLE =
      new DqeTableHandle("t", null, List.of("t"), 1L, null);

  private static final List<DqeColumnHandle> EMPLOYEE_COLUMNS =
      List.of(
          new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, "name.keyword", false),
          new DqeColumnHandle("age", "age", DqeTypes.INTEGER, true, null, false),
          new DqeColumnHandle("salary", "salary", DqeTypes.DOUBLE, true, null, false));

  @BeforeEach
  void setUp() {
    analyzer = new DqeAnalyzer();
    parser = new DqeSqlParser();

    metadata = mock(DqeMetadata.class);
    when(metadata.getTableHandle("default", "employees")).thenReturn(EMPLOYEES_TABLE);
    when(metadata.getTableHandle("default", "t")).thenReturn(T_TABLE);
    when(metadata.getTableHandle(
            eq("default"), argThat(name -> !"employees".equals(name) && !"t".equals(name))))
        .thenThrow(new DqeTableNotFoundException("unknown"));
    when(metadata.getColumnHandles(any(DqeTableHandle.class))).thenReturn(EMPLOYEE_COLUMNS);

    allowAllSecurity =
        new SecurityContext() {
          @Override
          public boolean hasIndexReadPermission(String indexName) {
            return true;
          }

          @Override
          public String getUserName() {
            return "test_user";
          }
        };

    denyAllSecurity =
        new SecurityContext() {
          @Override
          public boolean hasIndexReadPermission(String indexName) {
            return false;
          }

          @Override
          public String getUserName() {
            return "denied_user";
          }
        };
  }

  @Nested
  @DisplayName("Basic SELECT analysis")
  class BasicSelect {

    @Test
    @DisplayName("SELECT * FROM table produces all columns")
    void selectStar() {
      Statement stmt = parser.parse("SELECT * FROM employees");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals("employees", result.getTable().getIndexName());
      assertTrue(result.isSelectAll());
      assertEquals(3, result.getOutputColumnNames().size());
      assertTrue(result.getOutputColumnNames().contains("name"));
      assertTrue(result.getOutputColumnNames().contains("age"));
      assertTrue(result.getOutputColumnNames().contains("salary"));
    }

    @Test
    @DisplayName("SELECT specific columns")
    void selectSpecificColumns() {
      Statement stmt = parser.parse("SELECT name, age FROM employees");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertFalse(result.isSelectAll());
      assertEquals(2, result.getOutputColumnNames().size());
      assertEquals("name", result.getOutputColumnNames().get(0));
      assertEquals("age", result.getOutputColumnNames().get(1));
      assertEquals(DqeTypes.VARCHAR, result.getOutputColumnTypes().get(0));
      assertEquals(DqeTypes.INTEGER, result.getOutputColumnTypes().get(1));
    }

    @Test
    @DisplayName("SELECT with column alias")
    void selectWithAlias() {
      Statement stmt = parser.parse("SELECT name AS full_name FROM employees");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals(1, result.getOutputColumnNames().size());
      assertEquals("full_name", result.getOutputColumnNames().get(0));
    }

    @Test
    @DisplayName("SELECT with table alias")
    void selectWithTableAlias() {
      Statement stmt = parser.parse("SELECT e.name FROM employees e");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals(1, result.getOutputColumnNames().size());
      // Qualified reference: column name derived from expression toString()
      assertNotNull(result.getOutputColumnNames().get(0));
      assertEquals(DqeTypes.VARCHAR, result.getOutputColumnTypes().get(0));
    }

    @Test
    @DisplayName("SELECT with arithmetic expression")
    void selectWithArithmetic() {
      Statement stmt = parser.parse("SELECT age + salary FROM employees");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals(1, result.getOutputColumnNames().size());
      assertEquals(DqeTypes.DOUBLE, result.getOutputColumnTypes().get(0));
    }
  }

  @Nested
  @DisplayName("WHERE clause analysis")
  class WhereClause {

    @Test
    @DisplayName("WHERE produces predicate analysis")
    void whereProducesPredicateAnalysis() {
      Statement stmt = parser.parse("SELECT * FROM employees WHERE age > 18");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertTrue(result.hasWhereClause());
      assertTrue(result.getPredicateAnalysis().isPresent());
    }

    @Test
    @DisplayName("No WHERE means no predicate analysis")
    void noWhereNoPredicate() {
      Statement stmt = parser.parse("SELECT * FROM employees");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertFalse(result.hasWhereClause());
      assertTrue(result.getPredicateAnalysis().isEmpty());
    }

    @Test
    @DisplayName("WHERE with non-boolean expression throws")
    void whereNonBooleanThrows() {
      Statement stmt = parser.parse("SELECT * FROM employees WHERE age");
      assertThrows(
          DqeAnalysisException.class, () -> analyzer.analyze(stmt, metadata, allowAllSecurity));
    }

    @Test
    @DisplayName("WHERE with equality produces pushdown predicate")
    void whereEqualityPushdown() {
      Statement stmt = parser.parse("SELECT * FROM employees WHERE name = 'Alice'");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertTrue(result.getPredicateAnalysis().isPresent());
      assertFalse(result.getPredicateAnalysis().get().getPushdownPredicates().isEmpty());
    }
  }

  @Nested
  @DisplayName("ORDER BY analysis")
  class OrderBy {

    @Test
    @DisplayName("ORDER BY produces sort specifications")
    void orderByProducesSortSpecs() {
      Statement stmt = parser.parse("SELECT * FROM employees ORDER BY age ASC");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals(1, result.getSortSpecifications().size());
      assertEquals("age", result.getSortSpecifications().get(0).getColumn().getFieldName());
    }

    @Test
    @DisplayName("No ORDER BY means empty sort specs")
    void noOrderByEmptySortSpecs() {
      Statement stmt = parser.parse("SELECT * FROM employees");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertTrue(result.getSortSpecifications().isEmpty());
    }

    @Test
    @DisplayName("Multiple ORDER BY columns")
    void multipleOrderByColumns() {
      Statement stmt = parser.parse("SELECT * FROM employees ORDER BY name ASC, age DESC");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals(2, result.getSortSpecifications().size());
    }
  }

  @Nested
  @DisplayName("LIMIT and OFFSET")
  class LimitOffset {

    @Test
    @DisplayName("LIMIT is parsed correctly")
    void limitParsed() {
      Statement stmt = parser.parse("SELECT * FROM employees LIMIT 10");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertTrue(result.getLimit().isPresent());
      assertEquals(10L, result.getLimit().getAsLong());
    }

    @Test
    @DisplayName("No LIMIT means empty")
    void noLimit() {
      Statement stmt = parser.parse("SELECT * FROM employees");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertTrue(result.getLimit().isEmpty());
    }

    @Test
    @DisplayName("LIMIT 0 is valid")
    void limitZero() {
      Statement stmt = parser.parse("SELECT * FROM employees LIMIT 0");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertTrue(result.getLimit().isPresent());
      assertEquals(0L, result.getLimit().getAsLong());
    }
  }

  @Nested
  @DisplayName("Pipeline decisions")
  class PipelineDecisions {

    @Test
    @DisplayName("No ORDER BY, no LIMIT -> SCAN_ONLY")
    void scanOnly() {
      Statement stmt = parser.parse("SELECT * FROM employees");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals(
          OperatorSelectionRule.PipelineStrategy.SCAN_ONLY,
          result.getPipelineDecision().getStrategy());
    }

    @Test
    @DisplayName("ORDER BY + LIMIT -> TOP_N")
    void topN() {
      Statement stmt = parser.parse("SELECT * FROM employees ORDER BY age LIMIT 10");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals(
          OperatorSelectionRule.PipelineStrategy.TOP_N, result.getPipelineDecision().getStrategy());
    }

    @Test
    @DisplayName("ORDER BY without LIMIT -> FULL_SORT")
    void fullSort() {
      Statement stmt = parser.parse("SELECT * FROM employees ORDER BY age");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals(
          OperatorSelectionRule.PipelineStrategy.FULL_SORT,
          result.getPipelineDecision().getStrategy());
    }

    @Test
    @DisplayName("LIMIT without ORDER BY -> LIMIT_ONLY")
    void limitOnly() {
      Statement stmt = parser.parse("SELECT * FROM employees LIMIT 10");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals(
          OperatorSelectionRule.PipelineStrategy.LIMIT_ONLY,
          result.getPipelineDecision().getStrategy());
    }
  }

  @Nested
  @DisplayName("Required columns / projection pruning")
  class RequiredColumnsTests {

    @Test
    @DisplayName("SELECT * requires all columns")
    void selectStarRequiresAll() {
      Statement stmt = parser.parse("SELECT * FROM employees");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertTrue(result.getRequiredColumns().isAllColumns());
    }

    @Test
    @DisplayName("SELECT name requires only name column")
    void selectNameRequiresOnlyName() {
      Statement stmt = parser.parse("SELECT name FROM employees");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertFalse(result.getRequiredColumns().isAllColumns());
      assertEquals(1, result.getRequiredColumns().size());
    }

    @Test
    @DisplayName("WHERE references add to required columns")
    void whereReferencesAdded() {
      Statement stmt = parser.parse("SELECT name FROM employees WHERE age > 18");
      AnalyzedQuery result = analyzer.analyze(stmt, metadata, allowAllSecurity);

      assertEquals(2, result.getRequiredColumns().size());
    }
  }

  @Nested
  @DisplayName("Security")
  class Security {

    @Test
    @DisplayName("Denied access throws DqeAccessDeniedException")
    void deniedAccessThrows() {
      Statement stmt = parser.parse("SELECT * FROM employees");
      DqeAccessDeniedException ex =
          assertThrows(
              DqeAccessDeniedException.class,
              () -> analyzer.analyze(stmt, metadata, denyAllSecurity));
      assertThat(ex.getMessage(), containsString("employees"));
    }
  }

  @Nested
  @DisplayName("Unsupported constructs rejected by analyzer")
  class UnsupportedConstructs {

    @Test
    @DisplayName("GROUP BY rejected")
    void groupByRejected() {
      Statement stmt = parser.parse("SELECT department, COUNT(*) FROM t GROUP BY department");
      assertThrows(
          DqeUnsupportedOperationException.class,
          () -> analyzer.analyze(stmt, metadata, allowAllSecurity));
    }

    @Test
    @DisplayName("JOIN rejected")
    void joinRejected() {
      Statement stmt =
          parser.parse("SELECT a.name FROM employees a JOIN departments b ON a.id = b.id");
      assertThrows(
          DqeUnsupportedOperationException.class,
          () -> analyzer.analyze(stmt, metadata, allowAllSecurity));
    }

    @Test
    @DisplayName("DISTINCT rejected")
    void distinctRejected() {
      Statement stmt = parser.parse("SELECT DISTINCT name FROM employees");
      assertThrows(
          DqeUnsupportedOperationException.class,
          () -> analyzer.analyze(stmt, metadata, allowAllSecurity));
    }
  }

  @Nested
  @DisplayName("Error cases")
  class ErrorCases {

    @Test
    @DisplayName("Unknown table throws")
    void unknownTableThrows() {
      Statement stmt = parser.parse("SELECT * FROM nonexistent");
      assertThrows(Exception.class, () -> analyzer.analyze(stmt, metadata, allowAllSecurity));
    }

    @Test
    @DisplayName("Unknown column in SELECT throws")
    void unknownColumnThrows() {
      Statement stmt = parser.parse("SELECT nonexistent FROM employees");
      assertThrows(
          DqeAnalysisException.class, () -> analyzer.analyze(stmt, metadata, allowAllSecurity));
    }
  }
}
