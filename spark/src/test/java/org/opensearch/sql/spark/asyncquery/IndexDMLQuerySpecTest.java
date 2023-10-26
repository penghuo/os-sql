/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.net.URL;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.junit.Test;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class IndexDMLQuerySpecTest extends AsyncQueryExecutorServiceSpec {

  private final FlintDatasetMock SKIPPING = new FlintDatasetMock("DROP SKIPPING INDEX ON mys3" +
      ".default.http_logs", FlintIndexType.SKIPPING, "flint_mys3_default_http_logs_skipping_index");
  private final FlintDatasetMock COVERING = new FlintDatasetMock("DROP INDEX covering ON mys3.default.http_logs", FlintIndexType.COVERING, "flint_mys3_default_http_logs_covering_index");
  private final FlintDatasetMock MV = new FlintDatasetMock("DROP MATERIALIZED VIEW mv",
      FlintIndexType.MATERIALIZED_VIEW, "flint_mv");

  @Test
  public void basicDropAndFetchAndCancel() {
    ImmutableList.of(SKIPPING, COVERING, MV).forEach(mockDS -> {
      LocalEMRSClient emrsClient = new LocalEMRSClient() {
        @Override
        public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
          return new GetJobRunResult().withJobRun(new JobRun().withState("Cancelled"));
        }
      };

      AsyncQueryExecutorService asyncQueryExecutorService =
          createAsyncQueryExecutorService(emrsClient);

      // create skipping index
      mockDS.createIndex();

      // 1. drop index
      CreateAsyncQueryResponse response =
          asyncQueryExecutorService.createAsyncQuery(
              new CreateAsyncQueryRequest(mockDS.query, DATASOURCE
                  , LangType.SQL, null));

      assertNotNull(response.getQueryId());
      assertFalse(clusterService.state().routingTable().hasIndex(mockDS.indexName));

      AsyncQueryExecutionResponse asyncQueryResults =
          asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
      assertEquals("SUCCESS", asyncQueryResults.getStatus());
      assertNull(asyncQueryResults.getError());
      emrsClient.cancelJobRunCalled(1);

      IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
          () -> asyncQueryExecutorService.cancelQuery(response.getQueryId()));
      assertEquals("can't cancel index DML query", exception.getMessage());
    });
  }

  @Test
  public void dropIndexNoJobRunning() {
    ImmutableList.of(SKIPPING, COVERING, MV).forEach(mockDS -> {
      LocalEMRSClient emrsClient = new LocalEMRSClient() {
        @Override
        public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
          throw new IllegalArgumentException("Job run is not in a cancellable state");
        }
      };
      AsyncQueryExecutorService asyncQueryExecutorService =
          createAsyncQueryExecutorService(emrsClient);

      // create skipping index
      mockDS.createIndex();

      // 1. drop index
      CreateAsyncQueryResponse response =
          asyncQueryExecutorService.createAsyncQuery(
              new CreateAsyncQueryRequest(mockDS.query, DATASOURCE
                  , LangType.SQL, null));

      assertNotNull(response.getQueryId());
      assertFalse(clusterService.state().routingTable().hasIndex(mockDS.indexName));

      AsyncQueryExecutionResponse asyncQueryResults =
          asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
      assertEquals("SUCCESS", asyncQueryResults.getStatus());
      assertNull(asyncQueryResults.getError());
    });
  }

  @Test
  public void dropIndexCancelJobTimeout() {
    ImmutableList.of(SKIPPING, COVERING, MV).forEach(mockDS -> {
      LocalEMRSClient emrsClient = new LocalEMRSClient() {
        @Override
        public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
          return new GetJobRunResult().withJobRun(new JobRun().withState("Running"));
        }
      };
      AsyncQueryExecutorService asyncQueryExecutorService =
          createAsyncQueryExecutorService(emrsClient);

      // create skipping index
      mockDS.createIndex();

      // 1. drop index
      CreateAsyncQueryResponse response =
          asyncQueryExecutorService.createAsyncQuery(
              new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

      assertNotNull(response.getQueryId());
      assertTrue(clusterService.state().routingTable().hasIndex(mockDS.indexName));

      AsyncQueryExecutionResponse asyncQueryResults =
          asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
      assertEquals("FAILED", asyncQueryResults.getStatus());
      assertEquals("cancel job timeout", asyncQueryResults.getError());
    });
  }

  @RequiredArgsConstructor
  public class FlintDatasetMock {
    private final String query;
    private final FlintIndexType indexType;
    private final String indexName;

    public void createIndex() {
      switch (indexType) {
        case SKIPPING:
          createIndexWithMappings(indexName, loadSkippingIndexMappings());
          break;
        case COVERING:
          createIndexWithMappings(indexName, loadCoveringIndexMappings());
          break;
        case MATERIALIZED_VIEW:
          createIndexWithMappings(indexName, loadMV());
          break;
      }
    }
  }

  @SneakyThrows
  public static String loadSkippingIndexMappings() {
    URL url =
        Resources.getResource(
            "flint-index-mappings/flint_skipping_index.json");
    return Resources.toString(url, Charsets.UTF_8);
  }

  @SneakyThrows
  public static String loadCoveringIndexMappings() {
    URL url =
        Resources.getResource(
            "flint-index-mappings/flint_covering_index.json");
    return Resources.toString(url, Charsets.UTF_8);
  }

  @SneakyThrows
  public static String loadMV() {
    URL url =
        Resources.getResource(
            "flint-index-mappings/flint_mv.json");
    return Resources.toString(url, Charsets.UTF_8);
  }
}
