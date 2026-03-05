/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.projection.RequiredColumns;
import org.opensearch.dqe.analyzer.sort.OperatorSelectionRule;
import org.opensearch.dqe.analyzer.sort.PipelineDecision;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("AnalyzedQuery")
class AnalyzedQueryTest {

  @Test
  @DisplayName("Builder creates immutable AnalyzedQuery")
  void builderCreatesImmutableQuery() {
    DqeTableHandle table = new DqeTableHandle("test", null, List.of("test"), 1L, null);
    DqeColumnHandle col = new DqeColumnHandle("name", "name", DqeTypes.VARCHAR, true, null, false);
    RequiredColumns required = new RequiredColumns(Set.of(col));
    PipelineDecision decision =
        new PipelineDecision(
            OperatorSelectionRule.PipelineStrategy.SCAN_ONLY,
            List.of(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty());

    AnalyzedQuery query =
        AnalyzedQuery.builder()
            .table(table)
            .outputColumnNames(List.of("name"))
            .outputColumnTypes(List.of(DqeTypes.VARCHAR))
            .requiredColumns(required)
            .pipelineDecision(decision)
            .selectAll(false)
            .build();

    assertEquals("test", query.getTable().getIndexName());
    assertEquals(List.of("name"), query.getOutputColumnNames());
    assertEquals(List.of(DqeTypes.VARCHAR), query.getOutputColumnTypes());
    assertFalse(query.isSelectAll());
    assertFalse(query.hasWhereClause());
    assertTrue(query.getPredicateAnalysis().isEmpty());
    assertTrue(query.getLimit().isEmpty());
    assertTrue(query.getOffset().isEmpty());
    assertTrue(query.getSortSpecifications().isEmpty());
  }

  @Test
  @DisplayName("Builder with limit and offset")
  void builderWithLimitAndOffset() {
    DqeTableHandle table = new DqeTableHandle("test", null, List.of("test"), 1L, null);
    RequiredColumns required = new RequiredColumns(Set.of());
    PipelineDecision decision =
        new PipelineDecision(
            OperatorSelectionRule.PipelineStrategy.TOP_N,
            List.of(),
            OptionalLong.of(10),
            OptionalLong.of(20),
            OptionalLong.of(30));

    AnalyzedQuery query =
        AnalyzedQuery.builder()
            .table(table)
            .requiredColumns(required)
            .pipelineDecision(decision)
            .limit(10)
            .offset(20)
            .selectAll(true)
            .build();

    assertTrue(query.getLimit().isPresent());
    assertEquals(10L, query.getLimit().getAsLong());
    assertTrue(query.getOffset().isPresent());
    assertEquals(20L, query.getOffset().getAsLong());
    assertTrue(query.isSelectAll());
  }

  @Test
  @DisplayName("Output column lists are immutable")
  void outputColumnsImmutable() {
    DqeTableHandle table = new DqeTableHandle("test", null, List.of("test"), 1L, null);
    RequiredColumns required = new RequiredColumns(Set.of());
    PipelineDecision decision =
        new PipelineDecision(
            OperatorSelectionRule.PipelineStrategy.SCAN_ONLY,
            List.of(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty());

    AnalyzedQuery query =
        AnalyzedQuery.builder()
            .table(table)
            .outputColumnNames(List.of("name"))
            .outputColumnTypes(List.of(DqeTypes.VARCHAR))
            .requiredColumns(required)
            .pipelineDecision(decision)
            .build();

    assertThrows(
        UnsupportedOperationException.class, () -> query.getOutputColumnNames().add("extra"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> query.getOutputColumnTypes().add(DqeTypes.BIGINT));
  }

  @Test
  @DisplayName("Builder throws on null table")
  void builderThrowsOnNullTable() {
    RequiredColumns required = new RequiredColumns(Set.of());
    PipelineDecision decision =
        new PipelineDecision(
            OperatorSelectionRule.PipelineStrategy.SCAN_ONLY,
            List.of(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty());

    assertThrows(
        NullPointerException.class,
        () -> AnalyzedQuery.builder().requiredColumns(required).pipelineDecision(decision).build());
  }
}
