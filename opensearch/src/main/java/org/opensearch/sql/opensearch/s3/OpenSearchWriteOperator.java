/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

public class OpenSearchWriteOperator extends PhysicalPlan {

  private static final Logger LOG = LogManager.getLogger();

  private static int BATCH = 1000;

  @Getter
  private final PhysicalPlan input;

  private final String indexName;

  private final NodeClient nodeClient;


  private Iterator<Map<String, Object>> inputData;

  public OpenSearchWriteOperator(PhysicalPlan input, NodeClient nodeClient, String indexName) {
    this.input = input;
    this.nodeClient = nodeClient;
    this.indexName = indexName;
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitOpenSearchWriteOperator(this, context);
  }

  @Override
  public void close() {
    input.close();
  }

  @Override
  public void open() {
    input.open();

    List<Map<String, Object>> inputData = new ArrayList<>();
    while (input.hasNext()) {
      inputData.add((Map<String, Object>) input.next().value());
    }
    this.inputData = inputData.iterator();
  }

  @Override
  public boolean hasNext() {
    return inputData.hasNext() ;
  }

  @Override
  public ExprValue next() {
    try {
      // bulk write
      while (true) {
        Optional<BulkRequest> bulkRequest = bulkRequest(indexName);
        if (!bulkRequest.isPresent()) {
          break;
        }
        ActionFuture<BulkResponse> bulkResponseActionFuture =
            nodeClient.bulk(bulkRequest.get());
        BulkResponse bulkResponse = bulkResponseActionFuture.get();

        LOG.info("bulk took: {}", bulkResponse.getTook());
      }
      return ExprValueUtils.integerValue(0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Optional<BulkRequest> bulkRequest(String indexName) {
    if (!inputData.hasNext()) {
      return Optional.empty();
    }

    int cnt = 0;
    BulkRequest bulkRequest = new BulkRequest();
    while (inputData.hasNext() && cnt++ < BATCH) {
      bulkRequest.add(new IndexRequest(indexName).source(inputData.next()));
    }

    return Optional.of(bulkRequest);
  }
}
