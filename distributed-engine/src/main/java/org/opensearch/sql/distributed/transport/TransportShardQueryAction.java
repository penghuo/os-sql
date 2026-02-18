/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.transport;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.PagesSerde;
import org.opensearch.sql.distributed.exchange.OutputBuffer;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action handler for executing a plan fragment on a data node. Receives a serialized
 * StageFragment and shard list, executes the fragment locally, and returns the produced Pages.
 *
 * <p>In Phase 1, execution is synchronous: the full fragment is executed and all Pages are returned
 * in a single response. Phase 2 may add streaming support (multiple response chunks).
 */
@Log4j2
public class TransportShardQueryAction
    extends HandledTransportAction<ShardQueryRequest, ShardQueryResponse> {

  private final PagesSerde pagesSerde;

  @Inject
  public TransportShardQueryAction(TransportService transportService, ActionFilters actionFilters) {
    super(ShardQueryAction.NAME, transportService, actionFilters, ShardQueryRequest::new);
    this.pagesSerde = new PagesSerde();
  }

  @Override
  protected void doExecute(
      Task task, ShardQueryRequest request, ActionListener<ShardQueryResponse> listener) {
    try {
      log.debug(
          "Executing shard query: queryId={}, stageId={}, shards={}, index={}",
          request.getQueryId(),
          request.getStageId(),
          request.getShardIds(),
          request.getIndexName());

      // TODO: Phase 1 execution flow:
      // 1. Deserialize the StageFragment from request.getSerializedFragment()
      // 2. For each shard in request.getShardIds():
      //    a. Acquire IndexSearcher for the shard
      //    b. Build Operator pipeline from the fragment's PlanNode
      //    c. Execute the pipeline, collecting output Pages
      // 3. Serialize all Pages and return in response
      //
      // The actual pipeline execution will be wired once Driver/Pipeline (L1.7)
      // and the full operator chain are available. For now, this provides the
      // transport action skeleton that other components can depend on.

      OutputBuffer outputBuffer = new OutputBuffer();

      // Placeholder: execute fragment and collect pages from output buffer
      List<Page> resultPages = drainBuffer(outputBuffer);

      ShardQueryResponse response = new ShardQueryResponse(resultPages, false, pagesSerde);
      listener.onResponse(response);

    } catch (Exception e) {
      log.error(
          "Shard query execution failed: queryId={}, stageId={}",
          request.getQueryId(),
          request.getStageId(),
          e);
      listener.onFailure(e);
    }
  }

  /** Drain all pages from the output buffer. */
  private List<Page> drainBuffer(OutputBuffer buffer) {
    List<Page> pages = new ArrayList<>();
    Page page;
    while ((page = buffer.poll()) != null) {
      pages.add(page);
    }
    return pages;
  }
}
