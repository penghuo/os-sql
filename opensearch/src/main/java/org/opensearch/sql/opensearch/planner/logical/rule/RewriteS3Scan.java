/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.rest.RestStatus.OK;
import static org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.google.common.collect.ImmutableMap;
import java.util.Locale;
import org.apache.commons.lang3.RandomStringUtils;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.sql.opensearch.s3.transport.CreateViewAction;
import org.opensearch.sql.opensearch.s3.transport.CreateViewRequest;
import org.opensearch.sql.opensearch.s3.transport.CreateViewResponse;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.Rule;

public class RewriteS3Scan implements Rule<LogicalRelation> {

  private final NodeClient client;

  private final Capture<LogicalRelation> relationCapture;

  private final Pattern<LogicalRelation> pattern;

  /**
   * Constructor of MergeProjectAndRelation.
   */
  public RewriteS3Scan(NodeClient client) {
    this.client = client;
    this.relationCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalRelation.class)
        .matching(relation -> {
          return relation.getRelationName().toLowerCase(Locale.ROOT).startsWith("s3");
        });
  }

  @Override
  public Pattern<LogicalRelation> pattern() {
    return this.pattern;
  }

  @Override
  public LogicalPlan apply(LogicalRelation plan, Captures captures) {
    LogicalRelation relation = captures.get(relationCapture);
    String indexName = RandomStringUtils.random(10, false, true);

    ActionFuture<CreateViewResponse> actionFuture = client.execute(CreateViewAction.INSTANCE,
        new CreateViewRequest(
            relation.getRelationName(),
            indexName,
            new ImmutableMap.Builder<String, Object>()
                .put("@timestamp", "date")
                .put("clientip", "keyword")
                .put("request", "text")
                .put("size", "integer")
                .put("status", "integer")
                .build()
        ));
    try {
      if (actionFuture.get().isStatus()) {
        return new LogicalRelation(indexName);
      } else {
        return relation;
      }
    } catch (Exception e) {
      return relation;
    }
  }
}
