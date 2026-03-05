/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.core.rest.RestStatus.OK;

import com.alibaba.druid.sql.parser.ParserException;
import com.google.common.collect.ImmutableList;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.legacy.antlr.OpenSearchLegacySqlAnalyzer;
import org.opensearch.sql.legacy.antlr.SqlAnalysisConfig;
import org.opensearch.sql.legacy.antlr.SqlAnalysisException;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.cursor.CursorType;
import org.opensearch.sql.legacy.domain.ColumnTypeProvider;
import org.opensearch.sql.legacy.domain.QueryActionRequest;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.exception.SQLFeatureDisabledException;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.executor.ActionRequestRestExecutorFactory;
import org.opensearch.sql.legacy.executor.Format;
import org.opensearch.sql.legacy.executor.RestExecutor;
import org.opensearch.sql.legacy.executor.cursor.CursorActionRequestRestExecutorFactory;
import org.opensearch.sql.legacy.executor.cursor.CursorAsyncRestExecutor;
import org.opensearch.sql.legacy.executor.format.ErrorMessageFactory;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.sql.legacy.request.SqlRequestFactory;
import org.opensearch.sql.legacy.request.SqlRequestParam;
import org.opensearch.sql.legacy.rewriter.matchtoterm.VerificationException;
import org.opensearch.sql.legacy.utils.JsonPrettyFormatter;
import org.opensearch.sql.legacy.utils.QueryDataAnonymizer;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;

public class RestSqlAction extends BaseRestHandler {

  private static final Logger LOG = LogManager.getLogger(RestSqlAction.class);

  private final boolean allowExplicitIndex;

  private static final Predicate<String> CONTAINS_SUBQUERY =
      Pattern.compile("\\(\\s*select ").asPredicate();

  /** API endpoint path */
  public static final String QUERY_API_ENDPOINT = "/_plugins/_sql";

  public static final String EXPLAIN_API_ENDPOINT = QUERY_API_ENDPOINT + "/_explain";
  public static final String CURSOR_CLOSE_ENDPOINT = QUERY_API_ENDPOINT + "/close";

  /** New SQL query request handler. */
  private final RestSQLQueryAction newSqlQueryHandler;

  /**
   * DQE engine routing function. Accepts the request engine field (nullable) and returns true if
   * the request should be handled by DQE. May throw a RuntimeException (e.g. DqeException) if DQE
   * is disabled or the engine value is invalid. Defaults to always-false (DQE not wired).
   */
  private volatile Function<String, Boolean> dqeRoutingFunction = engine -> false;

  /**
   * DQE execution function. Accepts a JSON request body and returns a JSON response string. Set by
   * SQLPlugin after DqeEnginePlugin is initialized.
   */
  private volatile Function<String, String> dqeExecutionFunction = null;

  /**
   * DQE explain function. Accepts a JSON request body and returns an explain output string. Set by
   * SQLPlugin after DqeEnginePlugin is initialized.
   */
  private volatile Function<String, String> dqeExplainFunction = null;

  /**
   * DQE error formatter. Accepts a RuntimeException and returns a JSON error response string using
   * DQE's own error format (with actual error messages, not the legacy "Invalid SQL query" stub).
   * Set by SQLPlugin after DqeEnginePlugin is initialized.
   */
  private volatile Function<RuntimeException, String> dqeErrorFormatter = null;

  public RestSqlAction(Settings settings, Injector injector) {
    super();
    this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    this.newSqlQueryHandler = new RestSQLQueryAction(injector);
  }

  /**
   * Sets the DQE engine routing function. Called by SQLPlugin after DqeEnginePlugin is initialized
   * to wire EngineRouter.shouldUseDqe() into the REST handler.
   *
   * @param routingFunction function that accepts the request engine field and returns true for DQE
   */
  public void setDqeEngineRouting(Function<String, Boolean> routingFunction) {
    this.dqeRoutingFunction = routingFunction;
  }

  /**
   * Sets the DQE execution function. Called by SQLPlugin after DqeEnginePlugin is initialized. The
   * function accepts a JSON request body string and returns a JSON response string. Errors are
   * thrown as RuntimeException (DqeException).
   *
   * @param executionFunction function that executes a DQE query and returns JSON
   */
  public void setDqeExecutionFunction(Function<String, String> executionFunction) {
    this.dqeExecutionFunction = executionFunction;
  }

  /**
   * Sets the DQE explain function. Called by SQLPlugin after DqeEnginePlugin is initialized. The
   * function accepts a JSON request body string and returns an explain output string. Errors are
   * thrown as RuntimeException (DqeException).
   *
   * @param explainFunction function that explains a DQE query and returns output
   */
  public void setDqeExplainFunction(Function<String, String> explainFunction) {
    this.dqeExplainFunction = explainFunction;
  }

  /**
   * Sets the DQE error formatter. Called by SQLPlugin after DqeEnginePlugin is initialized. The
   * function accepts a RuntimeException and returns a JSON error response string using DQE's own
   * error format (preserving the actual error message in the "reason" field).
   *
   * @param errorFormatter function that formats a DQE error as JSON
   */
  public void setDqeErrorFormatter(Function<RuntimeException, String> errorFormatter) {
    this.dqeErrorFormatter = errorFormatter;
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(
        new Route(RestRequest.Method.POST, QUERY_API_ENDPOINT),
        new Route(RestRequest.Method.POST, EXPLAIN_API_ENDPOINT),
        new Route(RestRequest.Method.POST, CURSOR_CLOSE_ENDPOINT));
  }

  @Override
  public String getName() {
    return "sql_action";
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    Metrics.getInstance().getNumericalMetric(MetricName.REQ_TOTAL).increment();
    Metrics.getInstance().getNumericalMetric(MetricName.REQ_COUNT_TOTAL).increment();

    QueryContext.addRequestId();

    try {
      if (!isSQLFeatureEnabled()) {
        throw new SQLFeatureDisabledException(
            "Either plugins.sql.enabled or rest.action.multi.allow_explicit_index setting is"
                + " false");
      }

      final SqlRequest sqlRequest = SqlRequestFactory.getSqlRequest(request);
      if (isLegacyCursor(sqlRequest)) {
        if (isExplainRequest(request)) {
          throw new IllegalArgumentException("Invalid request. Cannot explain cursor");
        } else {
          LOG.info(
              "[{}] Cursor request {}: {}",
              QueryContext.getRequestId(),
              request.uri(),
              sqlRequest.cursor());
          return channel -> handleCursorRequest(request, sqlRequest.cursor(), client, channel);
        }
      }

      LOG.info("[{}] Incoming request {}", QueryContext.getRequestId(), request.uri());

      Format format = SqlRequestParam.getFormat(request.params());

      // Route request to new query engine if it's supported already
      SQLQueryRequest newSqlRequest =
          new SQLQueryRequest(
              sqlRequest.getJsonContent(),
              sqlRequest.getSql(),
              request.path(),
              request.params(),
              sqlRequest.cursor());

      // DQE engine routing: check if the request should be handled by DQE.
      // The routing function encapsulates EngineRouter.shouldUseDqe() logic, which:
      // - Respects the per-request "engine" field (takes precedence)
      // - Falls back to the "plugins.sql.engine" cluster setting
      // - Checks the "plugins.dqe.enabled" flag (throws if DQE is disabled)
      // - Validates engine field values (throws on invalid values like "spark")
      String requestEngine = newSqlRequest.getEngine();
      try {
        if (dqeRoutingFunction.apply(requestEngine)) {
          LOG.info("[{}] Request routed to DQE engine", QueryContext.getRequestId());

          // Route /_explain to the DQE explain path
          if (isExplainRequest(request)) {
            if (dqeExplainFunction == null) {
              return channel -> {
                channel.sendResponse(
                    new BytesRestResponse(
                        INTERNAL_SERVER_ERROR,
                        "application/json; charset=UTF-8",
                        "{\"error\":{\"reason\":\"DQE explain not initialized\","
                            + "\"type\":\"DqeException\","
                            + "\"engine\":\"dqe\"},"
                            + "\"status\":500}"));
              };
            }
            final String requestBody = sqlRequest.getJsonContent().toString();
            return channel -> {
              try {
                String explainOutput = dqeExplainFunction.apply(requestBody);
                channel.sendResponse(
                    new BytesRestResponse(OK, "application/json; charset=UTF-8", explainOutput));
              } catch (RuntimeException dqeError) {
                LOG.warn(
                    "[{}] DQE explain error: {}",
                    QueryContext.getRequestId(),
                    dqeError.getMessage());
                reportDqeError(channel, dqeError);
              }
            };
          }

          // Route query execution to the DQE engine
          if (dqeExecutionFunction == null) {
            return channel -> {
              channel.sendResponse(
                  new BytesRestResponse(
                      INTERNAL_SERVER_ERROR,
                      "application/json; charset=UTF-8",
                      "{\"error\":{\"reason\":\"DQE engine not initialized\","
                          + "\"type\":\"DqeException\","
                          + "\"engine\":\"dqe\"},"
                          + "\"status\":500}"));
            };
          }
          final String requestBody = sqlRequest.getJsonContent().toString();
          return channel -> {
            try {
              String jsonResponse = dqeExecutionFunction.apply(requestBody);
              channel.sendResponse(
                  new BytesRestResponse(OK, "application/json; charset=UTF-8", jsonResponse));
            } catch (RuntimeException dqeError) {
              LOG.warn(
                  "[{}] DQE execution error: {}",
                  QueryContext.getRequestId(),
                  dqeError.getMessage());
              reportDqeError(channel, dqeError);
            }
          };
        }
      } catch (RuntimeException dqeEx) {
        // DqeException (DQE_DISABLED, INVALID_REQUEST) — return as client error
        LOG.warn("[{}] DQE routing error: {}", QueryContext.getRequestId(), dqeEx.getMessage());
        return channel -> reportDqeError(channel, dqeEx);
      }

      return newSqlQueryHandler.prepareRequest(
          newSqlRequest,
          (restChannel, exception) -> {
            try {
              if (newSqlRequest.isExplainRequest()) {
                LOG.info(
                    "Request is falling back to old SQL engine due to: " + exception.getMessage());
              }
              LOG.info(
                  "[{}] Request {} is not supported and falling back to old SQL engine",
                  QueryContext.getRequestId(),
                  newSqlRequest);
              LOG.info("Request Query: {}", QueryDataAnonymizer.anonymizeData(sqlRequest.getSql()));
              QueryAction queryAction = explainRequest(client, sqlRequest, format);
              executeSqlRequest(request, queryAction, client, restChannel);
            } catch (Exception e) {
              handleException(restChannel, e);
            }
          },
          this::handleException);
    } catch (Exception e) {
      return channel -> handleException(channel, e);
    }
  }

  private void handleException(RestChannel restChannel, Exception exception) {
    logAndPublishMetrics(exception);
    if (exception instanceof OpenSearchException) {
      OpenSearchException openSearchException = (OpenSearchException) exception;
      reportError(restChannel, openSearchException, openSearchException.status());
    } else {
      reportError(
          restChannel, exception, isClientError(exception) ? BAD_REQUEST : INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * @param sqlRequest client request
   * @return true if this cursor was generated by the legacy engine, false otherwise.
   */
  private static boolean isLegacyCursor(SqlRequest sqlRequest) {
    String cursor = sqlRequest.cursor();
    return cursor != null && CursorType.getById(cursor.substring(0, 1)) != CursorType.NULL;
  }

  @Override
  protected Set<String> responseParams() {
    Set<String> responseParams = new HashSet<>(super.responseParams());
    responseParams.addAll(
        Arrays.asList(
            "sql", "flat", "separator", "_score", "_type", "_id", "newLine", "format", "sanitize"));
    return responseParams;
  }

  private void handleCursorRequest(
      final RestRequest request,
      final String cursor,
      final Client client,
      final RestChannel channel)
      throws Exception {
    CursorAsyncRestExecutor cursorRestExecutor =
        CursorActionRequestRestExecutorFactory.createExecutor(
            request, cursor, SqlRequestParam.getFormat(request.params()));
    cursorRestExecutor.execute(client, request.params(), channel);
  }

  private static void logAndPublishMetrics(final Exception e) {
    if (isClientError(e)) {
      LOG.error(QueryContext.getRequestId() + " Client side error during query execution", e);
      Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_CUS).increment();
    } else {
      LOG.error(QueryContext.getRequestId() + " Server side error during query execution", e);
      Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
    }
  }

  private static QueryAction explainRequest(
      final NodeClient client, final SqlRequest sqlRequest, Format format)
      throws SQLFeatureNotSupportedException, SqlParseException, SQLFeatureDisabledException {

    ColumnTypeProvider typeProvider = performAnalysis(sqlRequest.getSql());

    final QueryAction queryAction =
        new SearchDao(client)
            .explain(new QueryActionRequest(sqlRequest.getSql(), typeProvider, format));
    queryAction.setSqlRequest(sqlRequest);
    queryAction.setFormat(format);
    queryAction.setColumnTypeProvider(typeProvider);
    return queryAction;
  }

  private void executeSqlRequest(
      final RestRequest request,
      final QueryAction queryAction,
      final Client client,
      final RestChannel channel)
      throws Exception {
    Map<String, String> params = request.params();
    if (isExplainRequest(request)) {
      final String jsonExplanation = queryAction.explain().explain();
      String result;
      if (SqlRequestParam.isPrettyFormat(params)) {
        result = JsonPrettyFormatter.format(jsonExplanation);
      } else {
        result = jsonExplanation;
      }
      channel.sendResponse(new BytesRestResponse(OK, "application/json; charset=UTF-8", result));
    } else {
      RestExecutor restExecutor =
          ActionRequestRestExecutorFactory.createExecutor(SqlRequestParam.getFormat(params));
      // doing this hack because OpenSearch throws exception for un-consumed props
      Map<String, String> additionalParams = new HashMap<>();
      for (String paramName : responseParams()) {
        if (request.hasParam(paramName)) {
          additionalParams.put(paramName, request.param(paramName));
        }
      }
      restExecutor.execute(client, additionalParams, queryAction, channel);
    }
  }

  private static boolean isExplainRequest(final RestRequest request) {
    return request.path().endsWith("/_explain");
  }

  private static boolean isClientError(Exception e) {
    return e
            instanceof
            NullPointerException // NPE is hard to differentiate but more likely caused by bad query
        || e instanceof SqlParseException
        || e instanceof ParserException
        || e instanceof SQLFeatureNotSupportedException
        || e instanceof SQLFeatureDisabledException
        || e instanceof IllegalArgumentException
        || e instanceof IndexNotFoundException
        || e instanceof VerificationException
        || e instanceof SqlAnalysisException
        || e instanceof SyntaxCheckException
        || e instanceof SemanticCheckException
        || e instanceof ExpressionEvaluationException;
  }

  private void sendResponse(
      final RestChannel channel, final String message, final RestStatus status) {
    channel.sendResponse(new BytesRestResponse(status, message));
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    sendResponse(
        channel, ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString(), status);
  }

  /**
   * Report a DQE error using the DQE error formatter if available, falling back to the legacy error
   * formatter. The DQE formatter preserves the actual error message in the "reason" field instead
   * of replacing it with a generic "Invalid SQL query".
   */
  private void reportDqeError(final RestChannel channel, final RuntimeException e) {
    if (dqeErrorFormatter != null) {
      String errorJson = dqeErrorFormatter.apply(e);
      channel.sendResponse(
          new BytesRestResponse(BAD_REQUEST, "application/json; charset=UTF-8", errorJson));
    } else {
      reportError(channel, e, BAD_REQUEST);
    }
  }

  private boolean isSQLFeatureEnabled() {
    boolean isSqlEnabled =
        LocalClusterState.state()
            .getSettingValue(org.opensearch.sql.common.setting.Settings.Key.SQL_ENABLED);
    return allowExplicitIndex && isSqlEnabled;
  }

  private static ColumnTypeProvider performAnalysis(String sql) {
    LocalClusterState clusterState = LocalClusterState.state();
    SqlAnalysisConfig config = new SqlAnalysisConfig(false, false, 200);

    OpenSearchLegacySqlAnalyzer analyzer = new OpenSearchLegacySqlAnalyzer(config);
    Optional<Type> outputColumnType = analyzer.analyze(sql, clusterState);
    if (outputColumnType.isPresent()) {
      return new ColumnTypeProvider(outputColumnType.get());
    } else {
      return new ColumnTypeProvider();
    }
  }
}
