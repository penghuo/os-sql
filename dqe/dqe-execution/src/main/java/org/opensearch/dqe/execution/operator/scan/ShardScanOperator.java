/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator.scan;

import io.trino.spi.Page;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.dqe.execution.operator.Operator;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.dqe.execution.pit.PitHandle;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.types.converter.SearchHitToPageConverter;
import org.opensearch.search.SearchHit;
import org.opensearch.transport.client.Client;

/**
 * Shard-level scan operator that reads data from OpenSearch via the search API. Uses search_after
 * pagination (not scroll) with a PIT for consistent snapshots. Converts SearchHit batches to Pages.
 */
public class ShardScanOperator implements Operator {

  private final OperatorContext operatorContext;
  private final SearchRequestBuilder searchRequestBuilder;
  private final PitHandle pitHandle;
  private final Client client;
  private final int batchSize;
  private final SearchHitToPageConverter converter;

  private boolean finished = false;
  private Object[] lastSortValues = null;
  private boolean firstRequest = true;

  public ShardScanOperator(
      OperatorContext operatorContext,
      SearchRequestBuilder searchRequestBuilder,
      PitHandle pitHandle,
      Client client,
      int batchSize,
      SearchHitToPageConverter converter) {
    this.operatorContext =
        Objects.requireNonNull(operatorContext, "operatorContext must not be null");
    this.searchRequestBuilder =
        Objects.requireNonNull(searchRequestBuilder, "searchRequestBuilder must not be null");
    this.pitHandle = Objects.requireNonNull(pitHandle, "pitHandle must not be null");
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.batchSize = batchSize;
    this.converter = Objects.requireNonNull(converter, "converter must not be null");
  }

  @Override
  public Page getOutput() {
    if (finished) {
      return null;
    }
    operatorContext.checkInterrupted();

    try {
      SearchRequest request;
      if (firstRequest) {
        request = searchRequestBuilder.buildInitialRequest(pitHandle);
        firstRequest = false;
      } else {
        request = searchRequestBuilder.buildSearchAfterRequest(pitHandle, lastSortValues);
      }

      SearchResponse response = client.search(request).actionGet();
      SearchHit[] hits = response.getHits().getHits();

      if (hits.length == 0) {
        finished = true;
        return null;
      }

      operatorContext.addInputPositions(hits.length);

      // Save last sort values for search_after pagination
      lastSortValues = hits[hits.length - 1].getSortValues();

      // Convert SearchHits to source maps for the converter
      List<Map<String, Object>> sourceMaps = new ArrayList<>(hits.length);
      for (SearchHit hit : hits) {
        sourceMaps.add(hit.getSourceAsMap());
      }

      Page page = converter.convert(sourceMaps);
      operatorContext.addOutputPositions(page.getPositionCount());

      // If fewer hits than batch size, we've exhausted the shard
      if (hits.length < batchSize) {
        finished = true;
      }

      return page;
    } catch (DqeException e) {
      throw e;
    } catch (Exception e) {
      String msg = e.getMessage();
      if (msg != null && msg.contains("point_in_time_expired")) {
        throw new DqeException(
            "PIT expired for index [" + searchRequestBuilder.getIndexName() + "]",
            DqeErrorCode.PIT_EXPIRED,
            e);
      }
      throw new DqeException(
          "Shard scan failed for ["
              + searchRequestBuilder.getIndexName()
              + "]["
              + searchRequestBuilder.getShardId()
              + "]: "
              + e.getMessage(),
          DqeErrorCode.EXECUTION_ERROR,
          e);
    }
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public void finish() {
    finished = true;
  }

  @Override
  public void close() {
    // PIT cleanup is managed by PitManager at the query level, not per-operator
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }
}
