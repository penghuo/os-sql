/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext.OperatorSnapshot;

class IncrementalEnumerableOperatorsTest {

  @Test
  void aggregateUsesOneLiveStateForSnapshotsAndFinalRows() {
    RelDataType outputType = mock(RelDataType.class);
    List<OperatorSnapshot> snapshots = new ArrayList<>();
    AtomicInteger consumedRows = new AtomicInteger();
    try (ProgressiveQueryContext.Scope ignored =
        ProgressiveQueryContext.open(observer(snapshots))) {
      Enumerable<Object[]> result =
          IncrementalEnumerableOperators.aggregate(
              countingEnumerable(
                  List.of(
                      new Object[] {"a", 10},
                      new Object[] {"b", 20},
                      new Object[] {"a", 30},
                      new Object[] {"b", 40}),
                  consumedRows),
              outputType,
              List.of(0),
              List.of(countCall(), avgCall(1)),
              2);

      List<Object[]> rows = materialize(result);

      assertEquals(2, snapshots.size());
      assertEquals(2, snapshots.getFirst().rowsConsumed());
      assertEquals(4, snapshots.getLast().rowsConsumed());
      assertArrayEquals(new Object[] {"a", 2L, 20D}, rows.get(0));
      assertArrayEquals(new Object[] {"b", 2L, 30D}, rows.get(1));
      assertEquals(4, consumedRows.get());
    }
  }

  @Test
  void eventStatsRevisesEarlierRowsFromTheSamePartitionState() {
    RelDataType outputType = mock(RelDataType.class);
    List<OperatorSnapshot> snapshots = new ArrayList<>();
    AtomicInteger consumedRows = new AtomicInteger();
    try (ProgressiveQueryContext.Scope ignored =
        ProgressiveQueryContext.open(observer(snapshots))) {
      Enumerable<Object[]> result =
          IncrementalEnumerableOperators.eventStats(
              countingEnumerable(
                  List.of(
                      new Object[] {1, "a", 10},
                      new Object[] {2, "a", 30},
                      new Object[] {3, "b", 100},
                      new Object[] {4, "a", 50}),
                  consumedRows),
              outputType,
              List.of(1),
              List.of(
                  countCall(),
                  avgCall(2),
                  call(SqlKind.MIN, List.of(2), SqlTypeName.INTEGER),
                  call(SqlKind.MAX, List.of(2), SqlTypeName.INTEGER)),
              2);

      List<Object[]> rows = materialize(result);

      assertEquals(2, snapshots.size());
      assertArrayEquals(
          new Object[] {1, "a", 10, 2L, 20D, 10, 30}, snapshots.getFirst().rows().getFirst());
      assertArrayEquals(new Object[] {1, "a", 10, 3L, 30D, 10, 50}, rows.get(0));
      assertArrayEquals(new Object[] {4, "a", 50, 3L, 30D, 10, 50}, rows.get(2));
      assertArrayEquals(new Object[] {3, "b", 100, 1L, 100D, 100, 100}, rows.get(3));
      assertEquals(4, snapshots.getLast().stateUpdates());
      assertEquals(4, consumedRows.get());
    }
  }

  @Test
  void aggregateSupportsMinMaxAndCheckedIntegralSum() {
    RelDataType outputType = mock(RelDataType.class);
    Enumerable<Object[]> result =
        IncrementalEnumerableOperators.aggregate(
            countingEnumerable(
                Arrays.asList(
                    new Object[] {"a", 3L},
                    new Object[] {"a", null},
                    new Object[] {"a", 1L},
                    new Object[] {"a", 2L}),
                new AtomicInteger()),
            outputType,
            List.of(0),
            List.of(
                call(SqlKind.MIN, List.of(1), SqlTypeName.BIGINT),
                call(SqlKind.MAX, List.of(1), SqlTypeName.BIGINT),
                call(PPLBuiltinOperators.CHECKED_LONG_SUM, List.of(1), SqlTypeName.BIGINT)),
            2);

    assertArrayEquals(new Object[] {"a", 1L, 3L, 6L}, materialize(result).getFirst());
  }

  @Test
  void globalAggregatePreservesEmptyInputSemantics() {
    RelDataType outputType = mock(RelDataType.class);
    Enumerable<Object[]> result =
        IncrementalEnumerableOperators.aggregate(
            countingEnumerable(List.of(), new AtomicInteger()),
            outputType,
            List.of(),
            List.of(
                countCall(),
                call(SqlKind.SUM, List.of(0), SqlTypeName.BIGINT),
                call(SqlKind.SUM0, List.of(0), SqlTypeName.BIGINT),
                call(SqlKind.MIN, List.of(0), SqlTypeName.BIGINT),
                call(SqlKind.MAX, List.of(0), SqlTypeName.BIGINT)),
            2);

    assertArrayEquals(new Object[] {0L, null, 0L, null, null}, materialize(result).getFirst());
  }

  @Test
  void checkedIntegralSumFailsOnIntermediateOverflow() {
    RelDataType outputType = mock(RelDataType.class);
    Enumerable<Object[]> result =
        IncrementalEnumerableOperators.aggregate(
            countingEnumerable(
                List.of(new Object[] {Long.MAX_VALUE}, new Object[] {1L}), new AtomicInteger()),
            outputType,
            List.of(),
            List.of(call(PPLBuiltinOperators.CHECKED_LONG_SUM, List.of(0), SqlTypeName.BIGINT)),
            2);

    assertThrows(ArithmeticException.class, () -> materialize(result));
  }

  @Test
  void runningWindowEmitsStableRowsWithoutWaitingForSourceEof() {
    RelDataType outputType = mock(RelDataType.class);
    List<OperatorSnapshot> snapshots = new ArrayList<>();
    AtomicInteger consumedRows = new AtomicInteger();
    try (ProgressiveQueryContext.Scope ignored =
        ProgressiveQueryContext.open(observer(snapshots))) {
      Enumerable<Object[]> result =
          IncrementalEnumerableOperators.runningWindow(
              new Object(),
              countingEnumerable(
                  List.of(
                      new Object[] {"a", 10L},
                      new Object[] {"a", 20L},
                      new Object[] {"b", 5L},
                      new Object[] {"a", 30L}),
                  consumedRows),
              outputType,
              List.of(0),
              List.of(
                  call(SqlKind.ROW_NUMBER, List.of(), SqlTypeName.BIGINT),
                  call(SqlKind.SUM, List.of(1), SqlTypeName.BIGINT)));

      List<Object[]> rows = materialize(result);

      assertArrayEquals(new Object[] {"a", 10L, 1L, 10L}, rows.get(0));
      assertArrayEquals(new Object[] {"a", 20L, 2L, 30L}, rows.get(1));
      assertArrayEquals(new Object[] {"b", 5L, 1L, 5L}, rows.get(2));
      assertArrayEquals(new Object[] {"a", 30L, 3L, 60L}, rows.get(3));
      assertEquals(4, consumedRows.get());
    }
  }

  @Test
  void dedupKeepsOnlyAllowedOccurrencesAndAllNullKeys() {
    RelDataType outputType = mock(RelDataType.class);
    List<OperatorSnapshot> snapshots = new ArrayList<>();
    AtomicInteger consumedRows = new AtomicInteger();
    try (ProgressiveQueryContext.Scope ignored =
        ProgressiveQueryContext.open(observer(snapshots))) {
      Enumerable<Object[]> result =
          IncrementalEnumerableOperators.dedup(
              new Object(),
              countingEnumerable(
                  Arrays.asList(
                      new Object[] {"a", 1},
                      new Object[] {"a", 2},
                      new Object[] {"a", 3},
                      new Object[] {null, 4},
                      new Object[] {null, 5},
                      new Object[] {"b", 6}),
                  consumedRows),
              outputType,
              List.of(0),
              2,
              true,
              2);

      List<Object[]> rows = materialize(result);

      assertEquals(5, rows.size());
      assertArrayEquals(new Object[] {"a", 1}, rows.get(0));
      assertArrayEquals(new Object[] {"a", 2}, rows.get(1));
      assertArrayEquals(new Object[] {null, 4}, rows.get(2));
      assertArrayEquals(new Object[] {null, 5}, rows.get(3));
      assertArrayEquals(new Object[] {"b", 6}, rows.get(4));
      assertEquals(2, snapshots.size());
      assertEquals(6, consumedRows.get());
    }
  }

  @Test
  void topKUsesOneBoundedHeapForSnapshotsAndFinalRows() {
    RelDataType outputType = mock(RelDataType.class);
    List<OperatorSnapshot> snapshots = new ArrayList<>();
    AtomicInteger consumedRows = new AtomicInteger();
    try (ProgressiveQueryContext.Scope ignored =
        ProgressiveQueryContext.open(observer(snapshots))) {
      Enumerable<Object[]> result =
          IncrementalEnumerableOperators.topK(
              new Object(),
              countingEnumerable(
                  List.of(
                      new Object[] {"d", 4},
                      new Object[] {"a", 1},
                      new Object[] {"c", 3},
                      new Object[] {"b", 2},
                      new Object[] {"e", 5}),
                  consumedRows),
              outputType,
              row -> row[1],
              Comparator.comparingInt(value -> (Integer) value),
              1,
              2,
              2);

      List<Object[]> rows = materialize(result);

      assertArrayEquals(new Object[] {"b", 2}, rows.get(0));
      assertArrayEquals(new Object[] {"c", 3}, rows.get(1));
      assertEquals(2, snapshots.size());
      assertEquals(5, consumedRows.get());
    }
  }

  @Test
  void aggregateTopKAggregatePropagatesUpsertsAndRetractionsAcrossOperators() {
    RelDataType groupedType = mock(RelDataType.class);
    RelDataType topKType = mock(RelDataType.class);
    RelDataType totalType = mock(RelDataType.class);
    Object aggregateIdentity = new Object();
    Object topKIdentity = new Object();
    Object totalIdentity = new Object();
    List<OperatorSnapshot> snapshots = new ArrayList<>();

    try (ProgressiveQueryContext.Scope ignored =
        ProgressiveQueryContext.open(observer(snapshots))) {
      Enumerable<Object[]> grouped =
          IncrementalEnumerableOperators.aggregate(
              aggregateIdentity,
              countingEnumerable(
                  List.of(
                      new Object[] {"a"},
                      new Object[] {"a"},
                      new Object[] {"b"},
                      new Object[] {"b"},
                      new Object[] {"c"},
                      new Object[] {"c"},
                      new Object[] {"c"},
                      new Object[] {"c"}),
                  new AtomicInteger()),
              groupedType,
              List.of(0),
              List.of(countCall()),
              2);
      Enumerable<Object[]> topK =
          IncrementalEnumerableOperators.topK(
              topKIdentity,
              grouped,
              topKType,
              row -> row[1],
              Comparator.comparingLong(value -> (Long) value).reversed(),
              0,
              2,
              2);
      Enumerable<Object[]> total =
          IncrementalEnumerableOperators.aggregate(
              totalIdentity,
              topK,
              totalType,
              List.of(),
              List.of(call(SqlKind.SUM, List.of(1), SqlTypeName.BIGINT)),
              2);

      List<Object[]> rows = materialize(total);

      assertEquals(1, rows.size());
      assertArrayEquals(new Object[] {6L}, rows.getFirst());
      List<OperatorSnapshot> topKSnapshots =
          snapshots.stream()
              .filter(snapshot -> snapshot.operatorIdentity() == topKIdentity)
              .toList();
      assertEquals(3, topKSnapshots.size());
      assertArrayEquals(new Object[] {"a", 2L}, topKSnapshots.getFirst().rows().getFirst());
      assertArrayEquals(new Object[] {"c", 4L}, topKSnapshots.getLast().rows().getFirst());
    }
  }

  @Test
  void aggregateDedupTopKComposesWithoutReplayingInput() {
    RelDataType rowType = mock(RelDataType.class);
    AtomicInteger consumedRows = new AtomicInteger();

    Enumerable<Object[]> grouped =
        IncrementalEnumerableOperators.aggregate(
            new Object(),
            countingEnumerable(
                List.of(
                    new Object[] {"a"},
                    new Object[] {"a"},
                    new Object[] {"b"},
                    new Object[] {"b"},
                    new Object[] {"c"},
                    new Object[] {"c"},
                    new Object[] {"c"},
                    new Object[] {"c"}),
                consumedRows),
            rowType,
            List.of(0),
            List.of(countCall()),
            2);
    Enumerable<Object[]> deduplicated =
        IncrementalEnumerableOperators.dedup(
            new Object(), grouped, rowType, List.of(1), 1, false, 2);
    Enumerable<Object[]> topK =
        IncrementalEnumerableOperators.topK(
            new Object(),
            deduplicated,
            rowType,
            row -> row[1],
            Comparator.comparingLong(value -> (Long) value).reversed(),
            0,
            2,
            2);

    List<Object[]> rows = materialize(topK);

    assertEquals(8, consumedRows.get());
    assertEquals(2, rows.size());
    assertArrayEquals(new Object[] {"c", 4L}, rows.get(0));
    assertArrayEquals(new Object[] {"a", 2L}, rows.get(1));
  }

  @Test
  void aggregateCalcTopKAggregatePropagatesFilterAndProjectionChanges() {
    RelDataType rowType = mock(RelDataType.class);
    AtomicInteger consumedRows = new AtomicInteger();

    Enumerable<Object[]> grouped =
        IncrementalEnumerableOperators.aggregate(
            new Object(),
            countingEnumerable(
                List.of(
                    new Object[] {"a"},
                    new Object[] {"a"},
                    new Object[] {"b"},
                    new Object[] {"c"},
                    new Object[] {"c"},
                    new Object[] {"c"},
                    new Object[] {"b"},
                    new Object[] {"b"}),
                consumedRows),
            rowType,
            List.of(0),
            List.of(countCall()),
            2);
    Enumerable<Object[]> calculated =
        IncrementalEnumerableOperators.calc(
            new Object(),
            grouped,
            rowType,
            row -> (Long) row[1] >= 2L,
            row -> new Object[] {row[0], (Long) row[1] * 10L});
    Enumerable<Object[]> topK =
        IncrementalEnumerableOperators.topK(
            new Object(),
            calculated,
            rowType,
            row -> row[1],
            Comparator.comparingLong(value -> (Long) value).reversed(),
            0,
            2,
            2);
    Enumerable<Object[]> total =
        IncrementalEnumerableOperators.aggregate(
            new Object(),
            topK,
            rowType,
            List.of(),
            List.of(call(SqlKind.SUM, List.of(1), SqlTypeName.BIGINT)),
            2);

    List<Object[]> rows = materialize(total);

    assertEquals(8, consumedRows.get());
    assertEquals(1, rows.size());
    assertArrayEquals(new Object[] {60L}, rows.getFirst());
  }

  @Test
  void aggregateEventStatsTopKPropagatesPartitionWideUpdates() {
    RelDataType rowType = mock(RelDataType.class);
    AtomicInteger consumedRows = new AtomicInteger();

    Enumerable<Object[]> grouped =
        IncrementalEnumerableOperators.aggregate(
            new Object(),
            countingEnumerable(
                List.of(
                    new Object[] {"a"},
                    new Object[] {"a"},
                    new Object[] {"b"},
                    new Object[] {"c"},
                    new Object[] {"c"},
                    new Object[] {"c"},
                    new Object[] {"b"},
                    new Object[] {"b"}),
                consumedRows),
            rowType,
            List.of(0),
            List.of(countCall()),
            2);
    Enumerable<Object[]> eventStats =
        IncrementalEnumerableOperators.eventStats(
            new Object(), grouped, rowType, List.of(), List.of(countCall()), 2);
    Enumerable<Object[]> topK =
        IncrementalEnumerableOperators.topK(
            new Object(),
            eventStats,
            rowType,
            row -> row[1],
            Comparator.comparingLong(value -> (Long) value).reversed(),
            0,
            2,
            2);

    List<Object[]> rows = materialize(topK);

    assertEquals(8, consumedRows.get());
    assertEquals(2, rows.size());
    assertArrayEquals(new Object[] {"b", 3L, 3L}, rows.get(0));
    assertArrayEquals(new Object[] {"c", 3L, 3L}, rows.get(1));
  }

  @Test
  void aggregateRunningWindowTopKRecomputesOnlyAffectedPartitionSuffix() {
    RelDataType rowType = mock(RelDataType.class);
    AtomicInteger consumedRows = new AtomicInteger();

    Enumerable<Object[]> grouped =
        IncrementalEnumerableOperators.aggregate(
            new Object(),
            countingEnumerable(
                List.of(
                    new Object[] {"a"},
                    new Object[] {"a"},
                    new Object[] {"b"},
                    new Object[] {"c"},
                    new Object[] {"c"},
                    new Object[] {"c"},
                    new Object[] {"b"},
                    new Object[] {"b"}),
                consumedRows),
            rowType,
            List.of(0),
            List.of(countCall()),
            2);
    Enumerable<Object[]> runningWindow =
        IncrementalEnumerableOperators.runningWindow(
            new Object(),
            grouped,
            rowType,
            List.of(),
            List.of(call(SqlKind.SUM, List.of(1), SqlTypeName.BIGINT)));
    Enumerable<Object[]> topK =
        IncrementalEnumerableOperators.topK(
            new Object(),
            runningWindow,
            rowType,
            row -> row[2],
            Comparator.comparingLong(value -> (Long) value).reversed(),
            0,
            2,
            2);

    List<Object[]> rows = materialize(topK);

    assertEquals(8, consumedRows.get());
    assertEquals(2, rows.size());
    assertArrayEquals(new Object[] {"c", 3L, 8L}, rows.get(0));
    assertArrayEquals(new Object[] {"b", 3L, 5L}, rows.get(1));
  }

  private static Enumerable<Object[]> countingEnumerable(
      List<Object[]> rows, AtomicInteger consumedRows) {
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new Enumerator<>() {
          private int index = -1;

          @Override
          public Object[] current() {
            return rows.get(index);
          }

          @Override
          public boolean moveNext() {
            if (++index >= rows.size()) {
              return false;
            }
            consumedRows.incrementAndGet();
            return true;
          }

          @Override
          public void reset() {
            index = -1;
          }

          @Override
          public void close() {}
        };
      }
    };
  }

  private static ProgressiveQueryContext.Observer observer(List<OperatorSnapshot> snapshots) {
    return new ProgressiveQueryContext.Observer() {
      @Override
      public void onProgress(QueryProgress progress) {}

      @Override
      public void onOperatorSnapshot(OperatorSnapshot snapshot) {
        snapshots.add(snapshot);
      }

      @Override
      public void onSearchTaskStarted(long operationId, Runnable cancelAction) {}

      @Override
      public void onSearchTaskFinished(long operationId) {}
    };
  }

  private static AggregateCall countCall() {
    return call(SqlKind.COUNT, List.of(), SqlTypeName.BIGINT);
  }

  private static AggregateCall avgCall(int ordinal) {
    return call(SqlKind.AVG, List.of(ordinal), SqlTypeName.DOUBLE);
  }

  private static AggregateCall call(
      SqlKind kind, List<Integer> arguments, SqlTypeName outputTypeName) {
    SqlAggFunction function = mock(SqlAggFunction.class);
    when(function.getKind()).thenReturn(kind);
    return call(function, arguments, outputTypeName);
  }

  private static AggregateCall call(
      SqlAggFunction function, List<Integer> arguments, SqlTypeName outputTypeName) {
    AggregateCall call = mock(AggregateCall.class);
    RelDataType outputType = mock(RelDataType.class);
    when(outputType.getSqlTypeName()).thenReturn(outputTypeName);
    when(call.getAggregation()).thenReturn(function);
    when(call.getArgList()).thenReturn(arguments);
    when(call.getType()).thenReturn(outputType);
    return call;
  }

  private static List<Object[]> materialize(Enumerable<Object[]> enumerable) {
    List<Object[]> rows = new ArrayList<>();
    try (Enumerator<Object[]> enumerator = enumerable.enumerator()) {
      while (enumerator.moveNext()) {
        rows.add(enumerator.current());
      }
    }
    return rows;
  }
}
