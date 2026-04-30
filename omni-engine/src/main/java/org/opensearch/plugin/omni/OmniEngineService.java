/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.omni;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.ProtocolHeaders;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.server.SessionContext;
import io.trino.server.protocol.Query;
import io.trino.server.protocol.QueryResultsResponse;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.session.ResourceEstimates;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.plugin.omni.adapter.QueryResponseAdapter;
import org.opensearch.plugin.omni.ppl.PplTranslator;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;

/**
 * Boundary between TransportPPLQueryAction and the vendored Trino engine.
 *
 * <p>Takes a PPL request, translates to SQL via PplTranslator, dispatches through Trino, and
 * adapts the client-protocol results back into ExecutionEngine.QueryResponse / ExplainResponse.
 * Ports the body of omni/feat/ppl's SubmitQueryAction.prepareRequest; REST layer removed.
 */
public class OmniEngineService {
  private static final Logger log = LogManager.getLogger(OmniEngineService.class);

  private static final String DEFAULT_CATALOG = "opensearch";
  private static final String DEFAULT_SCHEMA = "default";
  private static final Duration FETCH_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
  private static final DataSize FETCH_BUFFER = DataSize.of(1, DataSize.Unit.MEGABYTE);
  private static final long DISPATCH_TIMEOUT_SEC = 30;
  private static final long RESULT_TIMEOUT_SEC = 60;

  private final ServiceWiring wiring;

  public OmniEngineService(ServiceWiring wiring) {
    this.wiring = wiring;
  }

  public void execute(
      PPLQueryRequest request, ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      DispatchResult dispatch = dispatch(request, "");
      if (dispatch.earlyFailure() != null) {
        listener.onFailure(classify(dispatch.earlyFailure()));
        return;
      }
      CollectedResults collected = collectResults(dispatch);
      if (collected.error() != null) {
        listener.onFailure(classify(collected.error()));
        return;
      }
      listener.onResponse(QueryResponseAdapter.adapt(collected.columns(), collected.rows(), dispatch.rowType()));
    } catch (Throwable t) {
      // Catch Error (AssertionError from Calcite RelToSql etc.) so the OpenSearch node
      // doesn't exit on uncaught Error. Everything surfaces as a normal query failure.
      listener.onFailure(classifyThrowable(t));
    }
  }

  public void explain(
      PPLQueryRequest request,
      ExplainMode mode,
      ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    try {
      String prefix =
          switch (mode == null ? ExplainMode.STANDARD : mode) {
            case STANDARD -> "EXPLAIN (TYPE DISTRIBUTED) ";
            case SIMPLE -> "EXPLAIN (TYPE LOGICAL) ";
            case EXTENDED -> "EXPLAIN ANALYZE ";
            case COST -> "EXPLAIN (TYPE VALIDATE) ";
          };
      DispatchResult dispatch = dispatch(request, prefix);
      if (dispatch.earlyFailure() != null) {
        listener.onFailure(classify(dispatch.earlyFailure()));
        return;
      }
      CollectedResults collected = collectResults(dispatch);
      if (collected.error() != null) {
        listener.onFailure(classify(collected.error()));
        return;
      }
      StringBuilder plan = new StringBuilder();
      for (List<Object> row : collected.rows()) {
        for (Object cell : row) {
          if (cell != null) plan.append(cell);
        }
        plan.append('\n');
      }
      listener.onResponse(
          new ExecutionEngine.ExplainResponse(
              new ExecutionEngine.ExplainResponseNode(plan.toString())));
    } catch (Throwable t) {
      listener.onFailure(classifyThrowable(t));
    }
  }

  // ---- internal ----

  private DispatchResult dispatch(PPLQueryRequest request, String sqlPrefix) throws Exception {
    DispatchManager dispatchManager = wiring.getDispatchManager();
    Identity identity = Identity.ofUser("ppl");
    SessionContext sessionContext =
        new SessionContext(
            ProtocolHeaders.TRINO_HEADERS,
            Optional.of(DEFAULT_CATALOG),
            Optional.of(DEFAULT_SCHEMA),
            Optional.empty(),
            Optional.empty(),
            identity,
            identity,
            new SelectedRole(SelectedRole.Type.NONE, Optional.empty()),
            Optional.of("ppl-plugin"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            ImmutableSet.of(),
            // clientCapabilities — announce PARAMETRIC_DATETIME so Trino's server protocol
            // serializes timestamps without trying to round to precision 3 (which breaks on
            // non-millisecond precision timestamps and causes SERIALIZATION_ERROR).
            ImmutableSet.of(io.trino.client.ClientCapabilities.PARAMETRIC_DATETIME.toString()),
            new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            Optional.empty(),
            false,
            Optional.empty());

    QueryId translationId = dispatchManager.createQueryId();
    Session pplSession =
        wiring
            .getSessionSupplier()
            .createSession(translationId, Span.getInvalid(), sessionContext);
    PplTranslator.TranslatedQuery translated = wiring.getPplTranslator().translate(request.getRequest(), pplSession);
    String sql = translated.sql();
    RelDataType rowType = translated.rowType();
    log.info("PPL translated to SQL: {}", sql);

    QueryId queryId = dispatchManager.createQueryId();
    Slug slug = Slug.createNew();
    ListenableFuture<Void> created =
        dispatchManager.createQuery(
            queryId, Span.getInvalid(), slug, sessionContext, sqlPrefix + sql);
    created.get(DISPATCH_TIMEOUT_SEC, TimeUnit.SECONDS);
    dispatchManager.waitForDispatched(queryId).get(DISPATCH_TIMEOUT_SEC, TimeUnit.SECONDS);

    QueryState state = dispatchManager.getQueryInfo(queryId).getState();
    if (state.isDone() && state != QueryState.FINISHED) {
      Optional<QueryInfo> info = dispatchManager.getFullQueryInfo(queryId);
      String msg =
          info.flatMap(i -> Optional.ofNullable(i.getFailureInfo()))
              .map(f -> f.getMessage())
              .orElse("query failed in state " + state);
      String name =
          info.flatMap(i -> Optional.ofNullable(i.getErrorCode()))
              .map(c -> c.getName())
              .orElse("");
      return new DispatchResult(queryId, slug, null, null, new RuntimeException(name + ":" + msg));
    }

    Session session = wiring.getSqlQueryManager().getQuerySession(queryId);
    Query query =
        Query.create(
            session,
            slug,
            wiring.getSqlQueryManager(),
            Optional.empty(),
            wiring.getDirectExchangeClientSupplier(),
            wiring.getExchangeManagerRegistry(),
            wiring.getQueryResultsExecutor(),
            wiring.getQueryResultsTimeoutExecutor(),
            wiring.getBlockEncodingSerde());
    return new DispatchResult(queryId, slug, query, rowType, null);
  }

  private CollectedResults collectResults(DispatchResult dispatch) throws Exception {
    Query query = dispatch.query();
    UriInfo uriInfo = new MinimalUriInfo(URI.create("http://ppl"));
    long token = 0;
    List<Column> columns = null;
    List<List<Object>> rows = new ArrayList<>();
    QueryError error = null;
    while (true) {
      QueryResultsResponse response =
          query
              .waitForResults(token, uriInfo, FETCH_TIMEOUT, FETCH_BUFFER)
              .get(RESULT_TIMEOUT_SEC, TimeUnit.SECONDS);
      QueryResults results = response.queryResults();
      if (columns == null && results.getColumns() != null) columns = results.getColumns();
      if (results.getData() != null) {
        for (List<Object> row : results.getData()) rows.add(row);
      }
      if (results.getError() != null) error = results.getError();
      if (results.getNextUri() == null) break;
      token++;
    }
    query.markResultsConsumedIfReady();
    return new CollectedResults(columns == null ? List.of() : columns, rows, error);
  }

  /**
   * Classify any Throwable (including Error/AssertionError that escapes Calcite/Trino internals)
   * into a normal query exception so the OpenSearch node never exits on an uncaught Error.
   */
  private Exception classifyThrowable(Throwable cause) {
    if (cause instanceof Error error) {
      String name = error.getClass().getSimpleName();
      String msg = error.getMessage() == null ? name : error.getMessage();
      msg = translateErrorMessage(msg);
      QueryEngineException wrapped = new QueryEngineException(
              "Query failed (" + name + "): " + msg, error);
      log.error("Uncaught Error during PPL execution: {}", msg, error);
      return wrapped;
    }
    return classify(cause);
  }

  private Exception classify(Object cause) {
    if (cause instanceof QueryError err) {
      String name = err.getErrorName() == null ? "" : err.getErrorName();
      String msg = err.getMessage() == null ? name : err.getMessage();
      // Translate Trino error messages to OpenSearch conventions
      msg = translateErrorMessage(msg);
      if ("SYNTAX_ERROR".equals(name)) return new SyntaxCheckException(msg);
      if (name.startsWith("MISSING_")) return new SemanticCheckException(msg);
      return new QueryEngineException(msg);
    }
    if (cause instanceof SyntaxCheckException
        || cause instanceof SemanticCheckException
        || cause instanceof QueryEngineException) {
      return (Exception) cause;
    }
    if (cause instanceof Exception e) {
      String msg = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      msg = translateErrorMessage(msg);
      return new QueryEngineException(msg, e);
    }
    return new QueryEngineException(String.valueOf(cause));
  }

  /**
   * Translates Trino/Calcite error messages to match OpenSearch SQL error conventions.
   */
  private static String translateErrorMessage(String msg) {
    // "Table 'foo' not found" → "no such index [foo]"
    msg = java.util.regex.Pattern.compile("Table '([^']+)' not found")
            .matcher(msg)
            .replaceAll("no such index [$1]");
    // "Field [foo] not found" → keep as-is (OpenSearch convention)
    return msg;
  }

  private record DispatchResult(QueryId queryId, Slug slug, Query query, RelDataType rowType, Throwable earlyFailure) {}

  private record CollectedResults(
      List<Column> columns, List<List<Object>> rows, QueryError error) {}

  /** Minimal UriInfo — Query only calls getBaseUriBuilder(). */
  private static final class MinimalUriInfo implements UriInfo {
    private final URI baseUri;

    MinimalUriInfo(URI baseUri) {
      this.baseUri = baseUri;
    }

    @Override
    public UriBuilder getBaseUriBuilder() {
      return new SimpleUriBuilder(baseUri);
    }

    @Override
    public URI getBaseUri() {
      return baseUri;
    }

    @Override
    public String getPath() {
      return "";
    }

    @Override
    public String getPath(boolean decode) {
      return "";
    }

    @Override
    public List<PathSegment> getPathSegments() {
      return List.of();
    }

    @Override
    public List<PathSegment> getPathSegments(boolean decode) {
      return List.of();
    }

    @Override
    public URI getRequestUri() {
      return baseUri;
    }

    @Override
    public UriBuilder getRequestUriBuilder() {
      return new SimpleUriBuilder(baseUri);
    }

    @Override
    public URI getAbsolutePath() {
      return baseUri;
    }

    @Override
    public UriBuilder getAbsolutePathBuilder() {
      return new SimpleUriBuilder(baseUri);
    }

    @Override
    public MultivaluedMap<String, String> getPathParameters() {
      return new MultivaluedHashMap<>();
    }

    @Override
    public MultivaluedMap<String, String> getPathParameters(boolean decode) {
      return new MultivaluedHashMap<>();
    }

    @Override
    public MultivaluedMap<String, String> getQueryParameters() {
      return new MultivaluedHashMap<>();
    }

    @Override
    public MultivaluedMap<String, String> getQueryParameters(boolean decode) {
      return new MultivaluedHashMap<>();
    }

    @Override
    public List<String> getMatchedURIs() {
      return List.of();
    }

    @Override
    public List<String> getMatchedURIs(boolean decode) {
      return List.of();
    }

    @Override
    public List<Object> getMatchedResources() {
      return List.of();
    }

    @Override
    public URI resolve(URI uri) {
      return baseUri.resolve(uri);
    }

    @Override
    public URI relativize(URI uri) {
      return baseUri.relativize(uri);
    }
  }

  /**
   * Simple UriBuilder that doesn't require JAX-RS RuntimeDelegate. Query only uses replacePath(),
   * path(), replaceQuery(), and build().
   */
  private static class SimpleUriBuilder extends UriBuilder {
    private String scheme;
    private String host;
    private int port;
    private StringBuilder path;
    private String query;

    SimpleUriBuilder(URI base) {
      this.scheme = base.getScheme();
      this.host = base.getHost();
      this.port = base.getPort();
      this.path = new StringBuilder(base.getPath() != null ? base.getPath() : "");
      this.query = base.getQuery();
    }

    @Override
    public UriBuilder replacePath(String p) {
      this.path = new StringBuilder(p != null ? p : "");
      return this;
    }

    @Override
    public UriBuilder path(String p) {
      if (!path.isEmpty() && path.charAt(path.length() - 1) != '/') path.append('/');
      path.append(p);
      return this;
    }

    @Override
    public UriBuilder replaceQuery(String q) {
      this.query = (q != null && !q.isEmpty()) ? q : null;
      return this;
    }

    @Override
    public URI build(Object... values) {
      try {
        String portStr = (port > 0) ? ":" + port : "";
        String queryStr = (query != null) ? "?" + query : "";
        return new URI(scheme + "://" + host + portStr + path + queryStr);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // Unused methods — required by abstract class
    @Override
    public UriBuilder clone() {
      return new SimpleUriBuilder(build());
    }

    @Override
    public UriBuilder uri(URI uri) {
      return this;
    }

    @Override
    public UriBuilder uri(String uri) {
      return this;
    }

    @Override
    public UriBuilder scheme(String s) {
      this.scheme = s;
      return this;
    }

    @Override
    public UriBuilder schemeSpecificPart(String s) {
      return this;
    }

    @Override
    public UriBuilder userInfo(String s) {
      return this;
    }

    @Override
    public UriBuilder host(String s) {
      this.host = s;
      return this;
    }

    @Override
    public UriBuilder port(int p) {
      this.port = p;
      return this;
    }

    @Override
    public UriBuilder path(Class c) {
      return this;
    }

    @Override
    public UriBuilder path(Class c, String m) {
      return this;
    }

    @Override
    public UriBuilder path(java.lang.reflect.Method m) {
      return this;
    }

    @Override
    public UriBuilder segment(String... s) {
      return this;
    }

    @Override
    public UriBuilder replaceMatrix(String s) {
      return this;
    }

    @Override
    public UriBuilder matrixParam(String n, Object... v) {
      return this;
    }

    @Override
    public UriBuilder replaceMatrixParam(String n, Object... v) {
      return this;
    }

    @Override
    public UriBuilder queryParam(String n, Object... v) {
      return this;
    }

    @Override
    public UriBuilder replaceQueryParam(String n, Object... v) {
      return this;
    }

    @Override
    public UriBuilder fragment(String s) {
      return this;
    }

    @Override
    public URI buildFromMap(Map<String, ?> m) {
      return build();
    }

    @Override
    public URI buildFromMap(Map<String, ?> m, boolean e) {
      return build();
    }

    @Override
    public URI buildFromEncodedMap(Map<String, ?> m) {
      return build();
    }

    @Override
    public URI build(Object[] v, boolean e) {
      return build();
    }

    @Override
    public URI buildFromEncoded(Object... v) {
      return build();
    }

    @Override
    public String toTemplate() {
      return path.toString();
    }

    @Override
    public UriBuilder resolveTemplate(String n, Object v) {
      return this;
    }

    @Override
    public UriBuilder resolveTemplate(String n, Object v, boolean e) {
      return this;
    }

    @Override
    public UriBuilder resolveTemplateFromEncoded(String n, Object v) {
      return this;
    }

    @Override
    public UriBuilder resolveTemplates(Map<String, Object> m) {
      return this;
    }

    @Override
    public UriBuilder resolveTemplates(Map<String, Object> m, boolean e) {
      return this;
    }

    @Override
    public UriBuilder resolveTemplatesFromEncoded(Map<String, Object> m) {
      return this;
    }
  }
}
