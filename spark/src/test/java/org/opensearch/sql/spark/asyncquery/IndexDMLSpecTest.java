/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;



import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class IndexDMLSpecTest extends AsyncQueryExecutorServiceImplSpecTest {
  @Before
  public void setup() {
    super.setup();
  }

  @After
  public void clean() {
    super.clean();
  }

  @Test
  public void testGetQueryResponseOfDropIndex() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // disable session
    enableSession(false);

    // 1. create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("DROP INDEX size_year ON mys3.default.http_logs", DATASOURCE
                , LangType.SQL, null));
    assertNull(response.getSessionId());
    assertNotNull(response.getQueryId());


  }
}
