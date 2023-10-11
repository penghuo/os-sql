/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import org.junit.Assert;
import org.junit.Test;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.spark.execution.statestore.IndexStateStore;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class InteractiveSessionTest extends OpenSearchSingleNodeTestCase {

  private static final String indexName = "mockindex";

  @Test
  public void openSession() {
    createIndex(indexName);
    IndexStateStore stateStore = new IndexStateStore(indexName, client());
    InteractiveSession session = new InteractiveSession(stateStore);

    session.open();

    String sessionId = session.getSessionModel().getSessionID().getSessionId();
    assertFalse(sessionId.isEmpty());

    stateStore.get(sessionId, ActionListener.wrap(
        stateModel -> {
          assertEquals("{\"version\":\"1.0\",\"sessionType\":\"INTERACTIVE\",\"sessionID\":{},\"sessionState\":\"NOT_STARTED\",\"datasourceName\":\"dataSourceName\"}", stateModel.getSource());
        },
        exception -> Assert.fail()
    ));
  }
}
