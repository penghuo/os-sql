/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.SqlTaskManager.SqlTaskWithResults;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.trino.plugin.TrinoServiceHolder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Handles {@code trino:task/results} on the worker node. Fetches pages from a task's OutputBuffer
 * and returns them as opaque TRINO_PAGES binary.
 */
public class TransportTrinoTaskResultsAction
    extends HandledTransportAction<TrinoTaskResultsRequest, TrinoTaskResultsResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportTrinoTaskResultsAction.class);

  @Inject
  public TransportTrinoTaskResultsAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(
        TrinoTaskResultsAction.NAME,
        transportService,
        actionFilters,
        TrinoTaskResultsRequest::new);
  }

  @Override
  protected void doExecute(
      Task task,
      TrinoTaskResultsRequest request,
      ActionListener<TrinoTaskResultsResponse> listener) {
    try {
      if (!TrinoServiceHolder.isInitialized()) {
        listener.onFailure(new IllegalStateException("Trino engine not initialized"));
        return;
      }

      TaskId taskId = TaskId.valueOf(request.getTaskId());
      OutputBufferId bufferId = new OutputBufferId(request.getBufferId());
      DataSize maxSize = DataSize.ofBytes(request.getMaxSizeBytes());

      SqlTaskWithResults taskWithResults =
          TrinoServiceHolder.getInstance()
              .getTaskManager()
              .getTaskResults(taskId, bufferId, request.getToken(), maxSize);

      ListenableFuture<BufferResult> resultsFuture = taskWithResults.getResultsFuture();

      resultsFuture.addListener(
          () -> {
            try {
              BufferResult result = resultsFuture.get();
              byte[] pagesBytes = serializePages(result.getSerializedPages());

              listener.onResponse(
                  new TrinoTaskResultsResponse(
                      result.getTaskInstanceId(),
                      result.getToken(),
                      result.getNextToken(),
                      result.isBufferComplete(),
                      pagesBytes));
            } catch (ExecutionException e) {
              listener.onFailure(
                  e.getCause() != null
                      ? new RuntimeException(e.getCause())
                      : new RuntimeException(e));
            } catch (Exception e) {
              listener.onFailure(e);
            }
          },
          Runnable::run);

    } catch (Exception e) {
      LOG.error("Failed to get task results for taskId={}", request.getTaskId(), e);
      listener.onFailure(e);
    }
  }

  /**
   * Concatenate serialized Trino pages (Slice list) into a single byte array. Format: [4-byte
   * length][page bytes][4-byte length][page bytes]...
   */
  static byte[] serializePages(List<Slice> serializedPages) {
    if (serializedPages.isEmpty()) {
      return new byte[0];
    }
    int totalSize = 0;
    for (Slice page : serializedPages) {
      totalSize += 4 + page.length();
    }
    byte[] result = new byte[totalSize];
    int offset = 0;
    for (Slice page : serializedPages) {
      int len = page.length();
      result[offset++] = (byte) (len >>> 24);
      result[offset++] = (byte) (len >>> 16);
      result[offset++] = (byte) (len >>> 8);
      result[offset++] = (byte) len;
      page.getBytes(0, result, offset, len);
      offset += len;
    }
    return result;
  }
}
