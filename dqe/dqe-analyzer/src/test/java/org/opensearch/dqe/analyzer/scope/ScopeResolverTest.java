/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.scope;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.DqeAnalysisException;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("ScopeResolver")
class ScopeResolverTest {

  private ScopeResolver resolver;
  private DqeTableHandle table;
  private DqeColumnHandle nameCol;
  private DqeColumnHandle ageCol;
  private DqeColumnHandle addressCityCol;
  private Scope scope;

  @BeforeEach
  void setUp() {
    resolver = new ScopeResolver();
    table = new DqeTableHandle("employees", null, List.of("employees"), 1L, null);
    nameCol = new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, "name.keyword", false);
    ageCol = new DqeColumnHandle("age", "age", DqeTypes.INTEGER, true, null, false);
    addressCityCol =
        new DqeColumnHandle("city", "address.city", DqeTypes.VARCHAR, true, null, false);
    scope = new Scope(table, List.of(nameCol, ageCol, addressCityCol), Optional.empty());
  }

  @Nested
  @DisplayName("resolveColumn")
  class ResolveColumn {

    @Test
    @DisplayName("Resolves column by field name")
    void resolveByFieldName() {
      ResolvedField resolved = resolver.resolveColumn("name", scope);
      assertEquals(nameCol, resolved.getColumn());
      assertEquals(DqeTypes.VARCHAR, resolved.getType());
    }

    @Test
    @DisplayName("Resolves column case-insensitively")
    void resolveCaseInsensitive() {
      ResolvedField resolved = resolver.resolveColumn("NAME", scope);
      assertEquals(nameCol, resolved.getColumn());
    }

    @Test
    @DisplayName("Resolves column by field path")
    void resolveByFieldPath() {
      ResolvedField resolved = resolver.resolveColumn("address.city", scope);
      assertEquals(addressCityCol, resolved.getColumn());
    }

    @Test
    @DisplayName("Throws for unknown column")
    void throwsForUnknownColumn() {
      DqeAnalysisException ex =
          assertThrows(DqeAnalysisException.class, () -> resolver.resolveColumn("unknown", scope));
      assertTrue(ex.getMessage().contains("not found"));
    }

    @Test
    @DisplayName("Throws for column alias in non-ORDER-BY context")
    void throwsForColumnAlias() {
      scope.addColumnAlias("total", DqeTypes.BIGINT);
      DqeAnalysisException ex =
          assertThrows(DqeAnalysisException.class, () -> resolver.resolveColumn("total", scope));
      assertTrue(ex.getMessage().contains("alias"));
    }
  }

  @Nested
  @DisplayName("resolveQualifiedColumn")
  class ResolveQualifiedColumn {

    @Test
    @DisplayName("Resolves table.column reference")
    void resolveTableDotColumn() {
      ResolvedField resolved = resolver.resolveQualifiedColumn("employees", "name", scope);
      assertEquals(nameCol, resolved.getColumn());
    }

    @Test
    @DisplayName("Resolves alias.column reference")
    void resolveAliasDotColumn() {
      Scope aliasedScope =
          new Scope(table, List.of(nameCol, ageCol), Optional.of("e"));
      ResolvedField resolved = resolver.resolveQualifiedColumn("e", "name", aliasedScope);
      assertEquals(nameCol, resolved.getColumn());
    }

    @Test
    @DisplayName("Throws for wrong qualifier")
    void throwsForWrongQualifier() {
      DqeAnalysisException ex =
          assertThrows(
              DqeAnalysisException.class,
              () -> resolver.resolveQualifiedColumn("wrong_table", "name", scope));
      assertTrue(ex.getMessage().contains("not found"));
    }

    @Test
    @DisplayName("Throws for unknown column with valid qualifier")
    void throwsForUnknownColumnWithValidQualifier() {
      DqeAnalysisException ex =
          assertThrows(
              DqeAnalysisException.class,
              () -> resolver.resolveQualifiedColumn("employees", "unknown", scope));
      assertTrue(ex.getMessage().contains("not found"));
    }
  }

  @Nested
  @DisplayName("Scope")
  class ScopeTests {

    @Test
    @DisplayName("Column aliases stored lowercase")
    void columnAliasesLowercase() {
      scope.addColumnAlias("Total", DqeTypes.BIGINT);
      assertTrue(scope.getColumnAliases().containsKey("total"));
    }

    @Test
    @DisplayName("getTableName returns index name")
    void getTableName() {
      assertEquals("employees", scope.getTableName());
    }

    @Test
    @DisplayName("Columns are immutable copy")
    void columnsImmutable() {
      assertThrows(
          UnsupportedOperationException.class,
          () -> scope.getColumns().add(nameCol));
    }
  }
}
