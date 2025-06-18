/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import java.time.ZoneId;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.tree.Paginate;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;

/** Query plan which includes a <em>select</em> query. */
public class QueryPlan extends AbstractPlan {

  /** The query plan ast. */
  protected final UnresolvedPlan plan;

  /** Query service. */
  protected final QueryService queryService;

  protected final ResponseListener<ExecutionEngine.QueryResponse> listener;

  protected final Optional<Integer> pageSize;

  protected final Optional<ZoneId> timezone;

  /** Constructor. */
  public QueryPlan(
      QueryId queryId,
      QueryType queryType,
      UnresolvedPlan plan,
      QueryService queryService,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    super(queryId, queryType);
    this.plan = plan;
    this.queryService = queryService;
    this.listener = listener;
    this.pageSize = Optional.empty();
    this.timezone = Optional.empty();
  }

  /** Constructor with timezone. */
  public QueryPlan(
      QueryId queryId,
      QueryType queryType,
      UnresolvedPlan plan,
      QueryService queryService,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      ZoneId timezone) {
    super(queryId, queryType);
    this.plan = plan;
    this.queryService = queryService;
    this.listener = listener;
    this.pageSize = Optional.empty();
    this.timezone = Optional.ofNullable(timezone);
  }

  /** Constructor with page size. */
  public QueryPlan(
      QueryId queryId,
      QueryType queryType,
      UnresolvedPlan plan,
      int pageSize,
      QueryService queryService,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    super(queryId, queryType);
    this.plan = plan;
    this.queryService = queryService;
    this.listener = listener;
    this.pageSize = Optional.of(pageSize);
    this.timezone = Optional.empty();
  }

  /** Constructor with page size and timezone. */
  public QueryPlan(
      QueryId queryId,
      QueryType queryType,
      UnresolvedPlan plan,
      int pageSize,
      QueryService queryService,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      ZoneId timezone) {
    super(queryId, queryType);
    this.plan = plan;
    this.queryService = queryService;
    this.listener = listener;
    this.pageSize = Optional.of(pageSize);
    this.timezone = Optional.ofNullable(timezone);
  }

  @Override
  public void execute() {
    if (pageSize.isPresent()) {
      if (timezone.isPresent()) {
        queryService.execute(
            new Paginate(pageSize.get(), plan), getQueryType(), listener, timezone.get());
      } else {
        queryService.execute(new Paginate(pageSize.get(), plan), getQueryType(), listener);
      }
    } else {
      if (timezone.isPresent()) {
        queryService.execute(plan, getQueryType(), listener, timezone.get());
      } else {
        queryService.execute(plan, getQueryType(), listener);
      }
    }
  }

  @Override
  public void explain(
      ResponseListener<ExecutionEngine.ExplainResponse> listener, Explain.ExplainFormat format) {
    if (pageSize.isPresent()) {
      listener.onFailure(
          new NotImplementedException(
              "`explain` feature for paginated requests is not implemented yet."));
    } else {
      queryService.explain(plan, getQueryType(), listener, format);
    }
  }
}
