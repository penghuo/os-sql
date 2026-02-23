/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.dqe.execution.pit.PitHandle;
import org.opensearch.dqe.execution.task.DqeQueryCancelledException;
import org.opensearch.dqe.memory.QueryMemoryBudget;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.types.converter.SearchHitToPageConverter;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.transport.client.Client;

@ExtendWith(MockitoExtension.class)
class ShardScanOperatorTests {

  @Mock private QueryMemoryBudget memoryBudget;
  @Mock private Client client;
  @Mock private SearchHitToPageConverter converter;
  @Mock private ActionFuture<SearchResponse> searchFuture;
  @Mock private SearchResponse searchResponse;
  @Mock private SearchHits searchHits;

  private OperatorContext operatorContext;
  private PitHandle pitHandle;
  private SearchRequestBuilder searchRequestBuilder;

  @BeforeEach
  void setUp() {
    operatorContext = new OperatorContext("q1", 0, 0, 0, "ShardScan", memoryBudget);
    pitHandle = new PitHandle("pit-123", "test-index", TimeValue.timeValueMinutes(5));
    searchRequestBuilder =
        new SearchRequestBuilder("test-index", 0, List.of("name"), null, null, 100);
  }

  private Page createDummyPage(int rows) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, rows);
    for (int i = 0; i < rows; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    return new Page(rows, builder.build());
  }

  private SearchHit createHit(Map<String, Object> source, Object... sortValues) {
    SearchHit hit = mock(SearchHit.class);
    lenient().when(hit.getSourceAsMap()).thenReturn(source);
    lenient().when(hit.getSortValues()).thenReturn(sortValues);
    return hit;
  }

  @Test
  @DisplayName("getOutput returns page from first batch")
  void getOutputReturnsPageFromFirstBatch() {
    SearchHit hit1 = createHit(Map.of("name", "Alice"), "sort1");
    SearchHit hit2 = createHit(Map.of("name", "Bob"), "sort2");
    SearchHit[] hits = {hit1, hit2};

    when(client.search(any(SearchRequest.class))).thenReturn(searchFuture);
    when(searchFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(hits);

    Page dummyPage = createDummyPage(2);
    when(converter.convert(any())).thenReturn(dummyPage);

    ShardScanOperator op =
        new ShardScanOperator(
            operatorContext, searchRequestBuilder, pitHandle, client, 100, converter);

    Page result = op.getOutput();
    assertNotNull(result);
    assertEquals(2, result.getPositionCount());
    // Fewer hits (2) than batch size (100) means shard is exhausted
    assertTrue(op.isFinished());
  }

  @Test
  @DisplayName("finishes when fewer hits than batch size")
  void finishesWhenFewerHitsThanBatchSize() {
    SearchHit hit1 = createHit(Map.of("name", "Alice"), "sort1");
    SearchHit[] hits = {hit1};

    when(client.search(any(SearchRequest.class))).thenReturn(searchFuture);
    when(searchFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(hits);

    Page dummyPage = createDummyPage(1);
    when(converter.convert(any())).thenReturn(dummyPage);

    ShardScanOperator op =
        new ShardScanOperator(
            operatorContext, searchRequestBuilder, pitHandle, client, 100, converter);

    Page result = op.getOutput();
    assertNotNull(result);
    assertTrue(op.isFinished());
  }

  @Test
  @DisplayName("finishes when zero hits returned")
  void finishesWhenZeroHits() {
    when(client.search(any(SearchRequest.class))).thenReturn(searchFuture);
    when(searchFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[0]);

    ShardScanOperator op =
        new ShardScanOperator(
            operatorContext, searchRequestBuilder, pitHandle, client, 100, converter);

    Page result = op.getOutput();
    assertNull(result);
    assertTrue(op.isFinished());
  }

  @Test
  @DisplayName("PIT expired error is detected and wrapped")
  void pitExpiredErrorDetected() {
    when(client.search(any(SearchRequest.class))).thenReturn(searchFuture);
    when(searchFuture.actionGet()).thenThrow(new RuntimeException("point_in_time_expired"));

    ShardScanOperator op =
        new ShardScanOperator(
            operatorContext, searchRequestBuilder, pitHandle, client, 100, converter);

    DqeException ex = assertThrows(DqeException.class, op::getOutput);
    assertEquals(DqeErrorCode.PIT_EXPIRED, ex.getErrorCode());
  }

  @Test
  @DisplayName("general search error is wrapped as EXECUTION_ERROR")
  void generalSearchErrorWrapped() {
    when(client.search(any(SearchRequest.class))).thenReturn(searchFuture);
    when(searchFuture.actionGet()).thenThrow(new RuntimeException("something broke"));

    ShardScanOperator op =
        new ShardScanOperator(
            operatorContext, searchRequestBuilder, pitHandle, client, 100, converter);

    DqeException ex = assertThrows(DqeException.class, op::getOutput);
    assertEquals(DqeErrorCode.EXECUTION_ERROR, ex.getErrorCode());
  }

  @Test
  @DisplayName("checkInterrupted throws when interrupted")
  void checkInterruptedThrows() {
    operatorContext.setInterrupted(true);

    ShardScanOperator op =
        new ShardScanOperator(
            operatorContext, searchRequestBuilder, pitHandle, client, 100, converter);

    assertThrows(DqeQueryCancelledException.class, op::getOutput);
  }

  @Test
  @DisplayName("getOutput returns null when already finished")
  void getOutputReturnsNullWhenFinished() {
    when(client.search(any(SearchRequest.class))).thenReturn(searchFuture);
    when(searchFuture.actionGet()).thenReturn(searchResponse);
    when(searchResponse.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(new SearchHit[0]);

    ShardScanOperator op =
        new ShardScanOperator(
            operatorContext, searchRequestBuilder, pitHandle, client, 100, converter);

    op.getOutput(); // finishes
    assertNull(op.getOutput()); // already finished
  }

  @Test
  @DisplayName("finish() sets finished flag")
  void finishSetsFlag() {
    ShardScanOperator op =
        new ShardScanOperator(
            operatorContext, searchRequestBuilder, pitHandle, client, 100, converter);
    op.finish();
    assertTrue(op.isFinished());
  }

  @Test
  @DisplayName("getOperatorContext returns context")
  void getOperatorContextReturnsContext() {
    ShardScanOperator op =
        new ShardScanOperator(
            operatorContext, searchRequestBuilder, pitHandle, client, 100, converter);
    assertEquals(operatorContext, op.getOperatorContext());
  }
}
