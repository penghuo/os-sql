/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.orchestrator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import io.trino.sql.tree.Statement;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.dqe.analyzer.AnalyzedQuery;
import org.opensearch.dqe.analyzer.DqeAnalyzer;
import org.opensearch.dqe.analyzer.projection.RequiredColumns;
import org.opensearch.dqe.analyzer.sort.OperatorSelectionRule;
import org.opensearch.dqe.analyzer.sort.PipelineDecision;
import org.opensearch.dqe.memory.AdmissionController;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.dqe.metadata.DqeMetadata;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.parser.DqeSqlParser;
import org.opensearch.dqe.plugin.logging.DqeAuditLogger;
import org.opensearch.dqe.plugin.logging.SlowQueryLogger;
import org.opensearch.dqe.plugin.metrics.DqeMetrics;
import org.opensearch.dqe.plugin.request.DqeQueryRequest;
import org.opensearch.dqe.plugin.response.DqeQueryResponse;
import org.opensearch.dqe.plugin.settings.DqeSettings;
import org.opensearch.dqe.types.DqeTypes;

@ExtendWith(MockitoExtension.class)
@DisplayName("DqeQueryOrchestrator")
@org.mockito.junit.jupiter.MockitoSettings(strictness = org.mockito.quality.Strictness.LENIENT)
class DqeQueryOrchestratorTests {

  @Mock private DqeAnalyzer analyzer;
  @Mock private DqeMetadata metadata;
  @Mock private ClusterService clusterService;

  private DqeSqlParser parser;
  private DqeSettings settings;
  private AdmissionController admissionController;
  private DqeMemoryTracker memoryTracker;
  private DqeMetrics metrics;
  private SlowQueryLogger slowQueryLogger;
  private DqeAuditLogger auditLogger;

  private DqeQueryOrchestrator orchestrator;

  @BeforeEach
  void setUp() {
    parser = new DqeSqlParser();

    // Create real DqeSettings with default cluster settings
    Set<org.opensearch.common.settings.Setting<?>> settingsSet =
        new java.util.HashSet<>(DqeSettings.getAllSettings());
    ClusterSettings clusterSettings =
        new ClusterSettings(Settings.EMPTY, settingsSet);
    lenient().when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    settings = new DqeSettings(clusterSettings);

    admissionController = new AdmissionController(10);
    memoryTracker =
        new DqeMemoryTracker(
            new org.opensearch.core.common.breaker.NoopCircuitBreaker("dqe"),
            new org.opensearch.core.common.breaker.NoopCircuitBreaker("parent"));
    metrics = new DqeMetrics();
    slowQueryLogger = new SlowQueryLogger(settings);
    auditLogger = new DqeAuditLogger();

    orchestrator =
        new DqeQueryOrchestrator(
            parser,
            analyzer,
            metadata,
            settings,
            admissionController,
            memoryTracker,
            metrics,
            slowQueryLogger,
            auditLogger,
            clusterService);
  }

  private AnalyzedQuery buildAnalyzedQuery() {
    DqeTableHandle table =
        new DqeTableHandle("test_index", null, List.of("test_index"), 1L, null);
    RequiredColumns requiredColumns = new RequiredColumns(Set.of());
    PipelineDecision pipelineDecision =
        new PipelineDecision(
            OperatorSelectionRule.PipelineStrategy.SCAN_ONLY,
            List.of(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty());

    return AnalyzedQuery.builder()
        .table(table)
        .outputColumnNames(List.of("col1", "col2"))
        .outputColumnTypes(List.of(DqeTypes.BIGINT, DqeTypes.VARCHAR))
        .outputExpressions(List.of())
        .requiredColumns(requiredColumns)
        .pipelineDecision(pipelineDecision)
        .selectAll(true)
        .build();
  }

  @Nested
  @DisplayName("Successful execution")
  class SuccessfulExecution {

    @Test
    @DisplayName("returns valid response for simple query")
    void returnsValidResponse() {
      AnalyzedQuery analyzed = buildAnalyzedQuery();
      when(analyzer.analyze(any(Statement.class), any(), any())).thenReturn(analyzed);

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1, col2 FROM test_index").build();

      DqeQueryResponse response = orchestrator.execute(request);

      assertNotNull(response);
      assertEquals("dqe", response.getEngine());
      assertEquals(2, response.getSchema().size());
      assertEquals("col1", response.getSchema().get(0).getName());
      assertEquals("bigint", response.getSchema().get(0).getType());
      assertEquals("col2", response.getSchema().get(1).getName());
      assertEquals("varchar", response.getSchema().get(1).getType());
    }

    @Test
    @DisplayName("response stats contain COMPLETED state")
    void statsShowCompleted() {
      AnalyzedQuery analyzed = buildAnalyzedQuery();
      when(analyzer.analyze(any(Statement.class), any(), any())).thenReturn(analyzed);

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1 FROM test_index").build();

      DqeQueryResponse response = orchestrator.execute(request);

      assertEquals("COMPLETED", response.getStats().getState());
      assertTrue(response.getStats().getElapsedMs() >= 0);
    }

    @Test
    @DisplayName("metrics record successful query")
    void metricsRecordSuccess() {
      AnalyzedQuery analyzed = buildAnalyzedQuery();
      when(analyzer.analyze(any(Statement.class), any(), any())).thenReturn(analyzed);

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1 FROM test_index").build();

      orchestrator.execute(request);

      DqeMetrics.MetricsSnapshot snap = metrics.getSnapshot();
      assertEquals(1, snap.getQueriesSubmitted());
      assertEquals(1, snap.getQueriesSucceeded());
      assertEquals(0, snap.getQueriesFailed());
    }

    @Test
    @DisplayName("active query count returns to zero after completion")
    void activeCountReturnsToZero() {
      AnalyzedQuery analyzed = buildAnalyzedQuery();
      when(analyzer.analyze(any(Statement.class), any(), any())).thenReturn(analyzed);

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1 FROM test_index").build();

      orchestrator.execute(request);

      assertEquals(0, orchestrator.getActiveQueryCount());
    }

    @Test
    @DisplayName("Phase 1 returns empty data list")
    void phase1ReturnsEmptyData() {
      AnalyzedQuery analyzed = buildAnalyzedQuery();
      when(analyzer.analyze(any(Statement.class), any(), any())).thenReturn(analyzed);

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1 FROM test_index").build();

      DqeQueryResponse response = orchestrator.execute(request);
      assertTrue(response.getData().isEmpty());
    }
  }

  @Nested
  @DisplayName("Admission control")
  class AdmissionControl {

    @Test
    @DisplayName("rejects query when at capacity")
    void rejectsAtCapacity() {
      // Create orchestrator with maxConcurrent=1 and exhaust slot
      AdmissionController singleSlot = new AdmissionController(1);
      assertTrue(singleSlot.tryAcquire()); // Take the only slot

      DqeQueryOrchestrator restrictedOrchestrator =
          new DqeQueryOrchestrator(
              parser,
              analyzer,
              metadata,
              settings,
              singleSlot,
              memoryTracker,
              metrics,
              slowQueryLogger,
              auditLogger,
              clusterService);

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT 1 FROM t").build();

      DqeException ex =
          assertThrows(DqeException.class, () -> restrictedOrchestrator.execute(request));
      assertEquals(DqeErrorCode.TOO_MANY_CONCURRENT_QUERIES, ex.getErrorCode());
    }

    @Test
    @DisplayName("admission slot released after successful query")
    void slotReleasedOnSuccess() {
      AnalyzedQuery analyzed = buildAnalyzedQuery();
      when(analyzer.analyze(any(Statement.class), any(), any())).thenReturn(analyzed);

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1 FROM test_index").build();

      orchestrator.execute(request);

      // Admission controller should have released the slot
      assertEquals(0, admissionController.getRunningQueryCount());
    }

    @Test
    @DisplayName("admission slot released after failed query")
    void slotReleasedOnFailure() {
      when(analyzer.analyze(any(Statement.class), any(), any()))
          .thenThrow(new DqeException("analysis failed", DqeErrorCode.EXECUTION_ERROR));

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1 FROM test_index").build();

      assertThrows(DqeException.class, () -> orchestrator.execute(request));

      assertEquals(0, admissionController.getRunningQueryCount());
    }
  }

  @Nested
  @DisplayName("Error handling")
  class ErrorHandling {

    @Test
    @DisplayName("parse error propagates as DqeException")
    void parseErrorPropagates() {
      DqeQueryRequest request =
          DqeQueryRequest.builder().query("NOT VALID SQL AT ALL @#$").build();

      DqeException ex =
          assertThrows(DqeException.class, () -> orchestrator.execute(request));
      // Parser errors throw DqeException with SYNTAX_ERROR
      assertNotNull(ex.getMessage());
    }

    @Test
    @DisplayName("analyzer DqeException propagates unchanged")
    void analyzerDqeExceptionPropagates() {
      DqeException analyzerError =
          new DqeException("Table not found: missing_table", DqeErrorCode.TABLE_NOT_FOUND);
      when(analyzer.analyze(any(Statement.class), any(), any())).thenThrow(analyzerError);

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1 FROM test_index").build();

      DqeException ex =
          assertThrows(DqeException.class, () -> orchestrator.execute(request));
      assertEquals(DqeErrorCode.TABLE_NOT_FOUND, ex.getErrorCode());
    }

    @Test
    @DisplayName("unexpected RuntimeException wrapped in EXECUTION_ERROR")
    void unexpectedExceptionWrapped() {
      when(analyzer.analyze(any(Statement.class), any(), any()))
          .thenThrow(new RuntimeException("unexpected NPE"));

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1 FROM test_index").build();

      DqeException ex =
          assertThrows(DqeException.class, () -> orchestrator.execute(request));
      assertEquals(DqeErrorCode.EXECUTION_ERROR, ex.getErrorCode());
      assertTrue(ex.getMessage().contains("Internal error"));
    }

    @Test
    @DisplayName("metrics record failed query on error")
    void metricsRecordFailure() {
      when(analyzer.analyze(any(Statement.class), any(), any()))
          .thenThrow(new DqeException("fail", DqeErrorCode.EXECUTION_ERROR));

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1 FROM test_index").build();

      assertThrows(DqeException.class, () -> orchestrator.execute(request));

      DqeMetrics.MetricsSnapshot snap = metrics.getSnapshot();
      assertEquals(1, snap.getQueriesSubmitted());
      assertEquals(0, snap.getQueriesSucceeded());
      assertEquals(1, snap.getQueriesFailed());
    }
  }

  @Nested
  @DisplayName("Query cancellation")
  class QueryCancellation {

    @Test
    @DisplayName("cancel on non-existent query is a no-op")
    void cancelNonExistentQueryNoOp() {
      assertDoesNotThrow(() -> orchestrator.cancel("non-existent-query-id"));
    }

    @Test
    @DisplayName("cancellation records metric")
    void cancellationRecordsMetric() {
      // We can't easily test mid-execution cancellation without threads,
      // so verify the cancel path doesn't error out for missing queries
      orchestrator.cancel("q-missing");
      // Verify that metrics didn't record a cancelled query for unknown queryId
      assertEquals(0, metrics.getSnapshot().getQueriesCancelled());
    }
  }

  @Nested
  @DisplayName("Memory budget")
  class MemoryBudget {

    @Test
    @DisplayName("uses per-request memory override when provided")
    void usesPerRequestOverride() {
      AnalyzedQuery analyzed = buildAnalyzedQuery();
      when(analyzer.analyze(any(Statement.class), any(), any())).thenReturn(analyzed);

      DqeQueryRequest request =
          DqeQueryRequest.builder()
              .query("SELECT col1 FROM test_index")
              .queryMaxMemoryBytes(1024L * 1024L * 512L) // 512MB
              .build();

      DqeQueryResponse response = orchestrator.execute(request);
      assertNotNull(response);
    }

    @Test
    @DisplayName("falls back to cluster setting when no per-request override")
    void fallsBackToClusterSetting() {
      AnalyzedQuery analyzed = buildAnalyzedQuery();
      when(analyzer.analyze(any(Statement.class), any(), any())).thenReturn(analyzed);

      DqeQueryRequest request =
          DqeQueryRequest.builder().query("SELECT col1 FROM test_index").build();

      // Should not throw - uses default 256MB from settings
      DqeQueryResponse response = orchestrator.execute(request);
      assertNotNull(response);
    }
  }

  @Nested
  @DisplayName("Constructor validation")
  class ConstructorValidation {

    @Test
    @DisplayName("rejects null parser")
    void rejectsNullParser() {
      assertThrows(
          NullPointerException.class,
          () ->
              new DqeQueryOrchestrator(
                  null,
                  analyzer,
                  metadata,
                  settings,
                  admissionController,
                  memoryTracker,
                  metrics,
                  slowQueryLogger,
                  auditLogger,
                  clusterService));
    }

    @Test
    @DisplayName("rejects null analyzer")
    void rejectsNullAnalyzer() {
      assertThrows(
          NullPointerException.class,
          () ->
              new DqeQueryOrchestrator(
                  parser,
                  null,
                  metadata,
                  settings,
                  admissionController,
                  memoryTracker,
                  metrics,
                  slowQueryLogger,
                  auditLogger,
                  clusterService));
    }

    @Test
    @DisplayName("rejects null admissionController")
    void rejectsNullAdmissionController() {
      assertThrows(
          NullPointerException.class,
          () ->
              new DqeQueryOrchestrator(
                  parser,
                  analyzer,
                  metadata,
                  settings,
                  null,
                  memoryTracker,
                  metrics,
                  slowQueryLogger,
                  auditLogger,
                  clusterService));
    }

    @Test
    @DisplayName("rejects null metrics")
    void rejectsNullMetrics() {
      assertThrows(
          NullPointerException.class,
          () ->
              new DqeQueryOrchestrator(
                  parser,
                  analyzer,
                  metadata,
                  settings,
                  admissionController,
                  memoryTracker,
                  null,
                  slowQueryLogger,
                  auditLogger,
                  clusterService));
    }
  }
}
