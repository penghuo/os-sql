/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;


import java.util.Optional;
import org.junit.Test;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class IndexStateStoreTest extends OpenSearchSingleNodeTestCase {

  private static final String indexName = "mockindex";
  private static IndexStateStore stateStore;

  @Test
  public void testCreateThenGet() {
//    createIndex(indexName);
//    stateStore = new IndexStateStore(indexName, client());
//
//    String expectedSource = "{\"data\":\"value\"}";
//    stateStore.create("0", expectedSource);
//    Optional<String> result = stateStore.get("0");
//
//    assertTrue(result.isPresent());
//    assertEquals(expectedSource, result.get());
  }
}
