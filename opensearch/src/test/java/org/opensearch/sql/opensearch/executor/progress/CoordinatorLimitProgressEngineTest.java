/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalDedup;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.progress.ProgressObserver;
import org.opensearch.sql.executor.progress.ProgressiveQueryResponseListener;
import org.opensearch.sql.executor.progress.ProgressiveSourceProgress;
import org.opensearch.sql.executor.progress.QueryProgress;
import org.opensearch.sql.executor.progress.SourceProgressEvent;
import org.opensearch.sql.monitor.ResourceStatus;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import org.opensearch.sql.opensearch.monitor.OpenSearchResourceMonitor;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/**
 * The integration gate for coordinator-limit progress, driven through the production engine.
 *
 * <h2>Why this test exists specifically</h2>
 *
 * A limit-driven completion signal can be wired perfectly at the wrapper and still never fire in
 * production, and neither the wrapper's own tests nor any "the job ends at 1.0" assertion will
 * notice. That is exactly what happened: the plan handed to the execution engine is still logical —
 * a {@code head} is a {@code LogicalSort}, and only the planner inside statement preparation
 * converts it — so an instrumentation pass that ran before the hand-off matched nothing. Everything
 * stayed green.
 *
 * <p>So this test runs the real {@link OpenSearchExecutionEngine} over the real planner, code
 * generator, registrar, and signal, with a fake client, and samples the published fraction <em>at
 * the moment the outer source issues its first search</em>. At that point the inner source — capped
 * by {@code head} behind a filter — is finished with its work, so it must already contribute its
 * full half. A regression that defers completion to the end of execution fails here and nowhere
 * else.
 */
class CoordinatorLimitProgressEngineTest {

  private static final double CEILING = QueryProgress.PUBLIC_CEILING.fractionDone();
  private static final double EPSILON = 1e-9;

  /** Rows each fake search returns, and the page size the fake index requests. */
  private static final int PAGE = 4;

  @Test
  @DisplayName(
      "a head-capped inner source contributes its full share before the outer source searches")
  void innerLimitCompletesBeforeOuterSearch() {
    RecordingObserver observer = new RecordingObserver();
    Listener listener = new Listener(observer);

    Map<String, Integer> searchesByIndex = new LinkedHashMap<>();
    AtomicInteger cleanups = new AtomicInteger();
    double[] fractionAtFirstOuterSearch = {Double.NaN};

    OpenSearchClient client =
        fakeClient(
            searchesByIndex,
            cleanups,
            index -> {
              if ("outer".equals(index) && searchesByIndex.get(index) == 1) {
                fractionAtFirstOuterSearch[0] = observer.current().fractionDone();
              }
            });

    CalcitePlanContext context = planContext(client);
    RelNode plan = coordinatorLimitPlan(context);
    new OpenSearchExecutionEngine(client, null, null).execute(plan, context, listener);

    assertNull(listener.failure, () -> "query failed: " + listener.failure);
    assertNotNull(listener.response, "query produced no response");
    assertEquals(
        2, observer.registered.size(), "both sources must be registered: " + observer.order);

    // The assertion that matters. The inner source is capped at two rows behind a filter, so no
    // static analysis of
    // the plan can bound the scan; only the limit reaching its own output quota can say the source
    // is finished. By
    // the time the outer source issues its first search, that must already have happened.
    assertTrue(
        Double.isFinite(fractionAtFirstOuterSearch[0]),
        "the outer source never searched, so the shape under test did not execute");
    assertEquals(
        CEILING * 0.5,
        fractionAtFirstOuterSearch[0],
        EPSILON,
        "the head-capped inner source must contribute its full share before the outer search;"
            + " deferring completion to the end of execution reports "
            + fractionAtFirstOuterSearch[0]);

    // And the completion came from the limit, not from teardown.
    assertTrue(
        observer.events.stream().anyMatch(SourceProgressEvent.SourceCompleted.class::isInstance),
        "a source completion event must have been published");
  }

  @Test
  @DisplayName("every source completes by the time execution finishes")
  void allSourcesCompleteByEndOfExecution() {
    RecordingObserver observer = new RecordingObserver();
    Listener listener = new Listener(observer);
    OpenSearchClient client = fakeClient(new LinkedHashMap<>(), new AtomicInteger(), index -> {});

    CalcitePlanContext context = planContext(client);
    new OpenSearchExecutionEngine(client, null, null)
        .execute(coordinatorLimitPlan(context), context, listener);

    assertNull(listener.failure);
    assertEquals(CEILING, observer.current().fractionDone(), EPSILON);
  }

  // ---------------------------------------------------------------- plan

  /**
   * The plan shape under test, built the way {@code QueryService} builds one: a non-equi join whose
   * right side is a {@code head}, a filter, and another {@code head}, wrapped in the system limit
   * and optimized.
   *
   * <p>Equivalent to {@code source=outer | inner join left=a, right=b ON a.id > b.id [source=logs |
   * head 100 | where id > 0 | head 2] | stats count()}.
   */
  private static RelNode coordinatorLimitPlan(CalcitePlanContext context) {
    RelBuilder builder = context.relBuilder;
    builder.scan("OpenSearch", "outer");

    // The subsearch: head 100, a filter, then head 2. The filter is what makes the inner cap
    // uninferable from the
    // scan's side — two raw rows need not yield two qualifying rows.
    builder.scan("OpenSearch", "logs");
    builder.limit(0, 100);
    builder.filter(
        builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("id"), builder.literal(0)));
    builder.limit(0, 2);

    // What CalciteRelNodeVisitor puts on a join subsearch: dedup on the join keys, then the
    // subsearch row cap. The
    // dedup is why the inner side is materialized before the outer scan starts, which is precisely
    // the window this
    // test samples — leaving it out changes the shape under test.
    RexNode dedupField = builder.field("id");
    RelNode right = builder.build();
    right = LogicalDedup.create(right, List.of(dedupField), 1, false, false);
    right =
        LogicalSystemLimit.create(
            LogicalSystemLimit.SystemLimitType.JOIN_SUBSEARCH_MAXOUT,
            right,
            builder.literal(context.sysLimit.joinSubsearchLimit()));
    builder.push(right);

    builder.join(
        JoinRelType.INNER,
        builder.call(
            SqlStdOperatorTable.GREATER_THAN,
            builder.field(2, 0, "id"),
            builder.field(2, 1, "id")));
    builder.aggregate(builder.groupKey(), builder.count(false, "c"));
    RelNode rel = builder.build();

    // Mirrors QueryService.convertToCalcitePlan: the system row limit, then a collation-preserving
    // sort when the
    // plan carries one, then the shared optimizer. Without these the shape under test is not the
    // shape that runs.
    rel =
        LogicalSystemLimit.create(
            LogicalSystemLimit.SystemLimitType.QUERY_SIZE_LIMIT,
            rel,
            builder.literal(context.sysLimit.querySizeLimit()));
    if (!(rel instanceof org.apache.calcite.rel.core.Sort)
        && rel.getTraitSet().getCollation() != RelCollations.EMPTY) {
      rel = LogicalSort.create(rel, rel.getTraitSet().getCollation(), null, null);
    }
    return CalciteToolsHelper.optimize(rel, context);
  }

  private static CalcitePlanContext planContext(OpenSearchClient client) {
    SchemaPlus schema = Frameworks.createRootSchema(true).add("OpenSearch", new AbstractSchema());
    schema.add("logs", new FakeIndex(client, "logs"));
    schema.add("outer", new FakeIndex(client, "outer"));
    return CalcitePlanContext.create(
        Frameworks.newConfigBuilder().defaultSchema(schema).build(),
        SysLimit.DEFAULT,
        QueryType.PPL);
  }

  // ---------------------------------------------------------------- fakes

  /** Notified with the index name each time a search starts, before its response is produced. */
  private interface SearchObserver {
    void onSearch(String index);
  }

  /**
   * A client that answers every search from memory. Deliberately returns a full page the first time
   * and nothing afterwards, so each source is exhausted on its second request and the test's timing
   * is not a race.
   */
  private static OpenSearchClient fakeClient(
      Map<String, Integer> searchesByIndex, AtomicInteger cleanups, SearchObserver searchObserver) {
    return (OpenSearchClient)
        Proxy.newProxyInstance(
            CoordinatorLimitProgressEngineTest.class.getClassLoader(),
            new Class<?>[] {OpenSearchClient.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "getNodeClient":
                  return Optional.empty();
                case "documentCountEstimate":
                  return Optional.of(new SourceEstimate(10_000, Map.of()));
                case "schedule":
                  ((Runnable) args[0]).run();
                  return null;
                case "search":
                  {
                    String index = ((OpenSearchQueryRequest) args[0]).getIndexName().toString();
                    int count = searchesByIndex.merge(index, 1, Integer::sum);
                    searchObserver.onSearch(index);
                    if (count > 1) {
                      return OpenSearchResponse.EMPTY;
                    }
                    ProgressiveQueryContext.openChannel().rowsObserved(PAGE, PAGE, 10_000);
                    return pageOfRows();
                  }
                case "forceCleanup":
                  cleanups.incrementAndGet();
                  return null;
                default:
                  return null;
              }
            });
  }

  private static OpenSearchResponse pageOfRows() {
    return new OpenSearchResponse(SearchHits.empty(), null, List.of(), false) {
      @Override
      public int getHitsSize() {
        return PAGE;
      }

      @Override
      public boolean isEmpty() {
        return false;
      }

      @Override
      public Iterator<ExprValue> iterator() {
        List<ExprValue> rows = new ArrayList<>();
        for (int id = 1; id <= PAGE; id++) {
          rows.add(ExprTupleValue.fromExprValueMap(Map.of("id", ExprValueUtils.integerValue(id))));
        }
        return rows.iterator();
      }
    };
  }

  /** Minimal index over the fake client, paging four rows at a time. */
  private static class FakeIndex extends OpenSearchIndex {

    private final String name;

    FakeIndex(OpenSearchClient client, String name) {
      super(client, new FakeSettings(), name);
      this.name = name;
    }

    @Override
    public Map<String, ExprType> getFieldTypes() {
      return Map.of("id", ExprCoreType.INTEGER);
    }

    @Override
    public Map<String, ExprType> getReservedFieldTypes() {
      return Map.of();
    }

    @Override
    public Map<String, ExprType> getAllFieldTypes() {
      return getFieldTypes();
    }

    @Override
    public Map<String, OpenSearchDataType> getFieldOpenSearchTypes() {
      return Map.of("id", OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer));
    }

    @Override
    public Map<String, String> getAliasMapping() {
      return Map.of();
    }

    @Override
    public Integer getMaxResultWindow() {
      return PAGE;
    }

    @Override
    public Integer getQueryBucketSize() {
      return 10;
    }

    @Override
    public OpenSearchRequest buildRequest(OpenSearchRequestBuilder builder) {
      // A point-in-time request, so the scan is classified as paged — the shape whose completion
      // otherwise waits.
      return OpenSearchQueryRequest.pitOf(
          new OpenSearchRequest.IndexName(name),
          new SearchSourceBuilder().size(PAGE),
          new OpenSearchExprValueFactory(getFieldOpenSearchTypes(), false),
          List.of(),
          TimeValue.timeValueMinutes(1),
          "pit");
    }

    @Override
    public OpenSearchResourceMonitor createOpenSearchResourceMonitor() {
      return new OpenSearchResourceMonitor(getSettings(), null) {
        @Override
        public ResourceStatus getStatus() {
          return ResourceStatus.healthy(ResourceStatus.ResourceType.OTHER);
        }
      };
    }
  }

  private static class FakeSettings extends Settings {
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getSettingValue(Key key) {
      return (T)
          switch (key) {
            case SQL_CURSOR_KEEP_ALIVE -> TimeValue.timeValueMinutes(1);
            case QUERY_SIZE_LIMIT -> Integer.MAX_VALUE;
            case QUERY_BUCKET_SIZE -> 10;
            case QUERY_MEMORY_LIMIT -> null;
            case CALCITE_PUSHDOWN_ENABLED, CALCITE_SUPPORT_ALL_JOIN_TYPES -> true;
            case CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR -> 0.1d;
            default -> false;
          };
    }

    @Override
    public List<?> getSettings() {
      return List.of();
    }
  }

  /** Records the engine's registration and event traffic so ordering violations are visible. */
  private static final class RecordingObserver implements ProgressObserver {

    private final ProgressiveSourceProgress calculator = new ProgressiveSourceProgress();
    private final List<Long> registered = new ArrayList<>();
    private final List<SourceProgressEvent> events = new ArrayList<>();
    private final List<String> order = new ArrayList<>();
    private int sealCalls;

    @Override
    public void register(long sourceId, OptionalLong estimatedDocs) {
      registered.add(sourceId);
      order.add("register:" + sourceId);
      calculator.register(sourceId, estimatedDocs);
    }

    @Override
    public void seal() {
      sealCalls++;
      order.add("seal");
      calculator.seal();
    }

    @Override
    public void accept(SourceProgressEvent event) {
      assertTrue(sealCalls > 0, "a source event arrived before registration was sealed: " + order);
      assertTrue(
          registered.contains(event.sourceId()),
          "event for an unregistered source " + event.sourceId() + ": " + order);
      events.add(event);
      order.add("event:" + event.sourceId());
      calculator.accept(event);
    }

    @Override
    public QueryProgress current() {
      return calculator.current();
    }
  }

  private static final class Listener
      implements ProgressiveQueryResponseListener<ExecutionEngine.QueryResponse> {

    private final ProgressObserver observer;
    private ExecutionEngine.QueryResponse response;
    private Throwable failure;

    Listener(ProgressObserver observer) {
      this.observer = observer;
    }

    @Override
    public ProgressObserver progressObserver() {
      return observer;
    }

    @Override
    public void onResponse(ExecutionEngine.QueryResponse value) {
      response = value;
    }

    @Override
    public void onFailure(Exception exception) {
      failure = exception;
    }
  }
}
