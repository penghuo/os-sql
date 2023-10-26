/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import java.util.Locale;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

/**
 * Delete Flint Index Operation.
 */
public class FlintIndexOpDelete extends FlintIndexOp {
  private static final Logger LOG = LogManager.getLogger();

  private final Client client;
  private final IndexQueryDetails indexDetails;

  public FlintIndexOpDelete(StateStore stateStore, String datasourceName, Client client,
                            IndexQueryDetails indexDetails) {
    super(stateStore, datasourceName);
    this.client = client;
    this.indexDetails = indexDetails;
  }

  public boolean validate(FlintIndexState state) {
    return state == FlintIndexState.ACTIVE || state == FlintIndexState.EMPTY;
  }

  @Override
  FlintIndexState transitioningState() {
    return FlintIndexState.DELETING;
  }

  @Override
  void runOp(FlintIndexStateModel flintIndex) {
    String indexName = indexDetails.openSearchIndexName();
    try {
      AcknowledgedResponse response =
          client.admin().indices().delete(new DeleteIndexRequest().indices(indexName)).get();
      if (!response.isAcknowledged()) {
        String errMsg = String.format(Locale.ROOT, "failed to delete index: %s", indexName);
        LOG.error(errMsg);
        throw new IllegalStateException(errMsg);
      }
    } catch (InterruptedException | ExecutionException e) {
      String errMsg = String.format(Locale.ROOT, "failed to delete index: %s", indexName);
      LOG.error(errMsg, e);
      throw new IllegalStateException(errMsg, e);
    }
  }

  @Override
  FlintIndexState stableState() {
    return FlintIndexState.DELETED;
  }
}
