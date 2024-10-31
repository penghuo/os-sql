/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.planner.physical.PhysicalPlan;

public class SparkExecutionEngine implements ExecutionEngine {

  private static final Logger LOG = LogManager.getLogger(SparkExecutionEngine.class);

  private LivySession session = null;

  public SparkExecutionEngine() {
    try {
      session = SecurityAccess.doPrivileged(LivySession::session);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execute(String query, ExecutionContext context,
                      ResponseListener<QueryResponse> listener) {
      SecurityAccess.doPrivileged(() -> {
        try {
          int queryId = session.submitQuery(query);
          listener.onResponse(session.getQueryResult(queryId));
        } catch (Exception e) {
          listener.onFailure(e);
        }
        return null;
      });
  }

  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute(PhysicalPlan plan, ExecutionContext context,
                      ResponseListener<QueryResponse> listener) {
    throw new UnsupportedOperationException();
  }


  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    throw new UnsupportedOperationException();
  }
}
