/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.rest.RestStatus.OK;
import static org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.opensearch.operator.transport.StreamExpressionAction;
import org.opensearch.sql.opensearch.operator.transport.StreamExpressionRequest;
import org.opensearch.sql.opensearch.operator.transport.StreamExpressionResponse;

public class RestStreamQueryAction extends BaseRestHandler {
  public static final String STREAM_API_ENDPOINT = "/_plugins/_stream";

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Constructor of RestPPLQueryAction.
   */
  public RestStreamQueryAction() {
    super();
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(new Route(RestRequest.Method.POST, STREAM_API_ENDPOINT));
  }

  @Override
  public String getName() {
    return "stream_expression_action";
  }

  @Override
  protected Set<String> responseParams() {
    Set<String> responseParams = new HashSet<>(super.responseParams());
    responseParams.addAll(Arrays.asList("format", "sanitize"));
    return responseParams;
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    return channel -> client.execute(
        StreamExpressionAction.INSTANCE,
        new StreamExpressionRequest(null, "stream()"),
        new ActionListener<StreamExpressionResponse>() {
          @Override
          public void onResponse(StreamExpressionResponse response) {
            channel.sendResponse(new BytesRestResponse(OK, "application/json; charset=UTF-8",
                response.getResult()));
          }

          @Override
          public void onFailure(Exception e) {
            channel.sendResponse(new BytesRestResponse(SERVICE_UNAVAILABLE, "application/json; "
                + "charset=UTF-8", e.getLocalizedMessage()));
          }
        });
  }

//  private ResponseListener<ExecutionEngine.QueryResponse> createListener(RestChannel channel,
//                                                                         PPLQueryRequest pplRequest) {
//    Format format = pplRequest.format();
//    ResponseFormatter<QueryResult> formatter;
//    if (format.equals(Format.CSV)) {
//      formatter = new CsvResponseFormatter(pplRequest.sanitize());
//    } else if (format.equals(Format.RAW)) {
//      formatter = new RawResponseFormatter();
//    } else if (format.equals(Format.VIZ)) {
//      formatter = new VisualizationResponseFormatter(pplRequest.style());
//    } else {
//      formatter = new SimpleJsonResponseFormatter(PRETTY);
//    }
//    return new ResponseListener<ExecutionEngine.QueryResponse>() {
//      @Override
//      public void onResponse(ExecutionEngine.QueryResponse response) {
//        sendResponse(channel, OK, formatter.format(new QueryResult(response.getSchema(),
//            response.getResults())));
//      }
//
//      @Override
//      public void onFailure(Exception e) {
//        LOG.error("Error happened during query handling", e);
//        if (isClientError(e)) {
//          Metrics.getInstance().getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_CUS).increment();
//          reportError(channel, e, BAD_REQUEST);
//        } else {
//          Metrics.getInstance().getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_SYS).increment();
//          reportError(channel, e, SERVICE_UNAVAILABLE);
//        }
//      }
//    };
//  }
//
//  private void sendResponse(RestChannel channel, RestStatus status, String content) {
//    channel.sendResponse(
//        new BytesRestResponse(status, "application/json; charset=UTF-8", content));
//  }
//
//  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
//    channel.sendResponse(new BytesRestResponse(status,
//        ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString()));
//  }
//
//  private static boolean isClientError(Exception e) {
//    return e instanceof NullPointerException
//        // NPE is hard to differentiate but more likely caused by bad query
//        || e instanceof IllegalArgumentException
//        || e instanceof IndexNotFoundException
//        || e instanceof SemanticCheckException
//        || e instanceof ExpressionEvaluationException
//        || e instanceof QueryEngineException
//        || e instanceof SyntaxCheckException;
//  }
}
