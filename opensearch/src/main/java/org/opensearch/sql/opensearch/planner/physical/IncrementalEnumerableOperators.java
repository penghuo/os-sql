/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext.OperatorSnapshot;

/**
 * Runtime for Calcite operators that retain the same state for partial and final output.
 *
 * <p>Unsupported aggregate calls are rejected by the planner rules and continue to use ordinary
 * Calcite operators. There is deliberately no replay fallback.
 */
public final class IncrementalEnumerableOperators {
  static final long DEFAULT_CHECKPOINT_ROWS = 10_000L;
  private static final double PROGRESSIVE_COST_MULTIPLIER = 0.01D;

  private IncrementalEnumerableOperators() {}

  /**
   * Makes the incremental equivalent win only inside progressive planning, where its rules are
   * enabled. Synchronous planning never registers a competing incremental expression.
   */
  static RelOptCost progressiveCost(RelOptCost ordinaryCost) {
    return ordinaryCost == null ? null : ordinaryCost.multiplyBy(PROGRESSIVE_COST_MULTIPLIER);
  }

  /**
   * Preserves the incremental channel while converting Calcite's generated child row format to
   * {@code Object[]}. Ordinary enumerable inputs remain ordinary append-only streams.
   */
  public static <T> Enumerable<Object[]> projectRows(
      Enumerable<T> input, Function1<T, Object[]> mapper) {
    if (!(input instanceof ChangeEnumerable changeInput)) {
      return input.select(mapper);
    }
    return new ChangeEnumerable() {
      @Override
      protected List<Object[]> compute(ChangeBatchConsumer consumer) {
        return changeInput
            .drain(
                batch -> {
                  List<RowChange> mapped = new ArrayList<>(batch.changes().size());
                  for (RowChange change : batch.changes()) {
                    mapped.add(
                        new RowChange(
                            change.identity(),
                            change.before() == null ? null : mapper.apply((T) change.before()),
                            change.after() == null ? null : mapper.apply((T) change.after())));
                  }
                  consumer.accept(
                      new ChangeBatch(mapped, batch.sourceRowsProcessed(), batch.terminal()));
                })
            .stream()
            .map(row -> mapper.apply((T) row))
            .toList();
      }
    };
  }

  static Enumerable<Object[]> calc(
      Object operatorIdentity,
      Enumerable<Object[]> input,
      RelDataType outputType,
      Predicate1<Object[]> predicate,
      Function1<Object[], Object[]> projector) {
    if (!(input instanceof ChangeEnumerable)) {
      return input.where(predicate).select(projector);
    }
    return new ChangeEnumerable() {
      @Override
      protected List<Object[]> compute(ChangeBatchConsumer consumer) {
        Map<RowIdentity, Object[]> current = new LinkedHashMap<>();
        long[] snapshotSequence = {0L};
        drainInput(
            input,
            checkpointRows(),
            batch -> {
              List<RowChange> outputChanges = new ArrayList<>();
              for (RowChange change : batch.changes()) {
                Object[] before =
                    change.before() != null && predicate.apply(change.before())
                        ? projector.apply(change.before())
                        : null;
                Object[] after =
                    change.after() != null && predicate.apply(change.after())
                        ? projector.apply(change.after())
                        : null;
                if (Arrays.deepEquals(before, after)) {
                  continue;
                }
                outputChanges.add(new RowChange(change.identity(), before, after));
                if (after == null) {
                  current.remove(change.identity());
                } else {
                  current.put(change.identity(), after.clone());
                }
              }
              List<Object[]> snapshot = current.values().stream().map(Object[]::clone).toList();
              if (!batch.terminal() && !snapshot.isEmpty() && !outputChanges.isEmpty()) {
                ProgressiveQueryContext.publishOperatorSnapshot(
                    new OperatorSnapshot(
                        operatorIdentity,
                        "CalciteEnumerableIncrementalCalc",
                        outputType,
                        snapshot,
                        batch.sourceRowsProcessed(),
                        current.size(),
                        ++snapshotSequence[0]));
              }
              consumer.accept(
                  new ChangeBatch(outputChanges, batch.sourceRowsProcessed(), batch.terminal()));
            });
        return current.values().stream().map(Object[]::clone).toList();
      }
    };
  }

  static Enumerable<Object[]> aggregate(
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> groupOrdinals,
      List<AggregateCall> calls) {
    return aggregate(new Object(), input, outputType, groupOrdinals, calls, checkpointRows());
  }

  static Enumerable<Object[]> aggregate(
      Object operatorIdentity,
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> groupOrdinals,
      List<AggregateCall> calls) {
    return aggregate(operatorIdentity, input, outputType, groupOrdinals, calls, checkpointRows());
  }

  static Enumerable<Object[]> aggregate(
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> groupOrdinals,
      List<AggregateCall> calls,
      long checkpointRows) {
    return aggregate(new Object(), input, outputType, groupOrdinals, calls, checkpointRows);
  }

  static Enumerable<Object[]> aggregate(
      Object operatorIdentity,
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> groupOrdinals,
      List<AggregateCall> calls,
      long checkpointRows) {
    return new ChangeEnumerable() {
      @Override
      protected List<Object[]> compute(ChangeBatchConsumer consumer) {
        LinkedHashMap<RowKey, AggregateGroupState> groups = new LinkedHashMap<>();
        Map<RowIdentity, Object[]> emitted = new LinkedHashMap<>();
        long[] nextOutputOrdinal = {0L};
        long[] snapshotSequence = {0L};
        if (groupOrdinals.isEmpty()) {
          groups.put(
              new RowKey(List.of()),
              new AggregateGroupState(
                  new RowIdentity(new RowKey(List.of()), nextOutputOrdinal[0]++),
                  createStates(calls)));
        }
        drainInput(
            input,
            checkpointRows,
            batch -> {
              Set<RowKey> dirtyGroups = new LinkedHashSet<>();
              if (groupOrdinals.isEmpty() && emitted.isEmpty()) {
                dirtyGroups.add(new RowKey(List.of()));
              }
              for (RowChange change : batch.changes()) {
                if (change.before() != null) {
                  removeAggregateInput(groups, dirtyGroups, groupOrdinals, calls, change.before());
                }
                if (change.after() != null) {
                  addAggregateInput(
                      groups, dirtyGroups, groupOrdinals, calls, change.after(), nextOutputOrdinal);
                }
              }

              List<RowChange> outputChanges = new ArrayList<>(dirtyGroups.size());
              for (RowKey groupKey : dirtyGroups) {
                AggregateGroupState group = groups.get(groupKey);
                RowIdentity identity =
                    group == null
                        ? emitted.keySet().stream()
                            .filter(id -> Objects.equals(id.key(), groupKey))
                            .findFirst()
                            .orElse(null)
                        : group.identity;
                Object[] before = identity == null ? null : emitted.get(identity);
                Object[] after = group == null ? null : aggregateRow(groupKey, group.states, calls);
                if (!Arrays.deepEquals(before, after)) {
                  outputChanges.add(new RowChange(identity, before, after));
                }
                if (identity != null) {
                  if (after == null) {
                    emitted.remove(identity);
                  } else {
                    emitted.put(identity, after);
                  }
                }
              }

              List<Object[]> snapshot = aggregateRows(groups, calls);
              if (!batch.terminal() && !snapshot.isEmpty() && !outputChanges.isEmpty()) {
                ProgressiveQueryContext.publishOperatorSnapshot(
                    new OperatorSnapshot(
                        operatorIdentity,
                        "CalciteEnumerableIncrementalAggregate",
                        outputType,
                        snapshot,
                        batch.sourceRowsProcessed(),
                        emitted.size(),
                        ++snapshotSequence[0]));
              }
              consumer.accept(
                  new ChangeBatch(outputChanges, batch.sourceRowsProcessed(), batch.terminal()));
            });
        return aggregateRows(groups, calls);
      }
    };
  }

  static Enumerable<Object[]> eventStats(
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> partitionOrdinals,
      List<AggregateCall> calls) {
    return eventStats(new Object(), input, outputType, partitionOrdinals, calls, checkpointRows());
  }

  static Enumerable<Object[]> eventStats(
      Object operatorIdentity,
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> partitionOrdinals,
      List<AggregateCall> calls) {
    return eventStats(
        operatorIdentity, input, outputType, partitionOrdinals, calls, checkpointRows());
  }

  static Enumerable<Object[]> eventStats(
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> partitionOrdinals,
      List<AggregateCall> calls,
      long checkpointRows) {
    return eventStats(new Object(), input, outputType, partitionOrdinals, calls, checkpointRows);
  }

  static Enumerable<Object[]> eventStats(
      Object operatorIdentity,
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> partitionOrdinals,
      List<AggregateCall> calls,
      long checkpointRows) {
    return new ChangeEnumerable() {
      @Override
      protected List<Object[]> compute(ChangeBatchConsumer consumer) {
        Map<RowIdentity, EventStatsStoredRow> rowsByIdentity = new LinkedHashMap<>();
        Map<RowKey, EventStatsPartition> partitions = new LinkedHashMap<>();
        Map<RowIdentity, Object[]> emitted = new LinkedHashMap<>();
        long[] snapshotSequence = {0L};

        drainInput(
            input,
            checkpointRows,
            batch -> {
              for (RowChange change : batch.changes()) {
                EventStatsStoredRow previous = rowsByIdentity.remove(change.identity());
                if (previous != null) {
                  EventStatsPartition partition = partitions.get(previous.partitionKey);
                  if (partition == null
                      || partition.rows.remove(change.identity().ordinal()) == null) {
                    throw new IllegalStateException(
                        "Eventstats retraction references an unknown row");
                  }
                  remove(partition.states, calls, previous.row);
                  if (partition.rows.isEmpty()) {
                    partitions.remove(previous.partitionKey);
                  }
                }
                if (change.after() != null) {
                  Object[] row = change.after().clone();
                  RowKey partitionKey = RowKey.from(row, partitionOrdinals);
                  EventStatsPartition partition =
                      partitions.computeIfAbsent(
                          partitionKey, ignored -> new EventStatsPartition(createStates(calls)));
                  if (partition.rows.put(change.identity().ordinal(), change.identity()) != null) {
                    throw new IllegalStateException(
                        "Eventstats received duplicate row identity ordinal");
                  }
                  add(partition.states, calls, row);
                  rowsByIdentity.put(change.identity(), new EventStatsStoredRow(partitionKey, row));
                }
              }

              Map<RowIdentity, Object[]> current =
                  eventStatsRows(rowsByIdentity, partitions, calls);
              List<RowChange> outputChanges = diffRows(emitted, current);
              emitted.clear();
              emitted.putAll(current);
              List<Object[]> snapshot = current.values().stream().map(Object[]::clone).toList();
              if (!batch.terminal() && !snapshot.isEmpty() && !outputChanges.isEmpty()) {
                ProgressiveQueryContext.publishOperatorSnapshot(
                    new OperatorSnapshot(
                        operatorIdentity,
                        "CalciteEnumerableIncrementalWindow",
                        outputType,
                        snapshot,
                        batch.sourceRowsProcessed(),
                        rowsByIdentity.size(),
                        ++snapshotSequence[0]));
              }
              consumer.accept(
                  new ChangeBatch(outputChanges, batch.sourceRowsProcessed(), batch.terminal()));
            });
        return emitted.values().stream().map(Object[]::clone).toList();
      }
    };
  }

  static Enumerable<Object[]> runningWindow(
      Object operatorIdentity,
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> partitionOrdinals,
      List<AggregateCall> calls) {
    return new ChangeEnumerable() {
      @Override
      protected List<Object[]> compute(ChangeBatchConsumer consumer) {
        Map<RowIdentity, RunningWindowStoredRow> rowsByIdentity = new LinkedHashMap<>();
        Map<RowKey, RunningWindowPartition> partitions = new LinkedHashMap<>();
        Map<RowIdentity, Object[]> emitted = new LinkedHashMap<>();
        long[] snapshotSequence = {0L};

        drainInput(
            input,
            checkpointRows(),
            batch -> {
              Map<RowKey, Long> firstDirtyOrdinal = new LinkedHashMap<>();
              for (RowChange change : batch.changes()) {
                RunningWindowStoredRow previous = rowsByIdentity.remove(change.identity());
                if (previous != null) {
                  RunningWindowPartition partition = partitions.get(previous.partitionKey);
                  if (partition == null
                      || partition.rows.remove(change.identity().ordinal()) == null) {
                    throw new IllegalStateException(
                        "Running window retraction references an unknown row");
                  }
                  firstDirtyOrdinal.merge(
                      previous.partitionKey, change.identity().ordinal(), Math::min);
                  if (partition.rows.isEmpty()) {
                    partitions.remove(previous.partitionKey);
                  }
                }
                if (change.after() != null) {
                  Object[] row = change.after().clone();
                  RowKey partitionKey = RowKey.from(row, partitionOrdinals);
                  RunningWindowPartition partition =
                      partitions.computeIfAbsent(
                          partitionKey, ignored -> new RunningWindowPartition());
                  if (partition.rows.put(
                          change.identity().ordinal(),
                          new RunningWindowStoredRow(
                              change.identity(), partitionKey, row, null, null))
                      != null) {
                    throw new IllegalStateException(
                        "Running window received duplicate row identity ordinal");
                  }
                  rowsByIdentity.put(
                      change.identity(),
                      new RunningWindowStoredRow(change.identity(), partitionKey, row, null, null));
                  firstDirtyOrdinal.merge(partitionKey, change.identity().ordinal(), Math::min);
                }
              }

              for (Map.Entry<RowKey, Long> dirty : firstDirtyOrdinal.entrySet()) {
                RunningWindowPartition partition = partitions.get(dirty.getKey());
                if (partition != null) {
                  recomputeRunningWindowSuffix(partition, dirty.getValue(), rowsByIdentity, calls);
                }
              }

              Map<RowIdentity, Object[]> current = runningWindowRows(rowsByIdentity);
              List<RowChange> outputChanges = diffRows(emitted, current);
              emitted.clear();
              emitted.putAll(current);
              List<Object[]> snapshot = current.values().stream().map(Object[]::clone).toList();
              if (!batch.terminal() && !snapshot.isEmpty() && !outputChanges.isEmpty()) {
                ProgressiveQueryContext.publishOperatorSnapshot(
                    new OperatorSnapshot(
                        operatorIdentity,
                        "CalciteEnumerableIncrementalWindow",
                        outputType,
                        snapshot,
                        batch.sourceRowsProcessed(),
                        rowsByIdentity.size(),
                        ++snapshotSequence[0]));
              }
              consumer.accept(
                  new ChangeBatch(outputChanges, batch.sourceRowsProcessed(), batch.terminal()));
            });
        return emitted.values().stream().map(Object[]::clone).toList();
      }
    };
  }

  static Enumerable<Object[]> dedup(
      Object operatorIdentity,
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> keyOrdinals,
      int allowedDuplication,
      boolean keepEmpty) {
    return dedup(
        operatorIdentity,
        input,
        outputType,
        keyOrdinals,
        allowedDuplication,
        keepEmpty,
        checkpointRows());
  }

  static Enumerable<Object[]> dedup(
      Object operatorIdentity,
      Enumerable<Object[]> input,
      RelDataType outputType,
      List<Integer> keyOrdinals,
      int allowedDuplication,
      boolean keepEmpty,
      long checkpointRows) {
    return new ChangeEnumerable() {
      @Override
      protected List<Object[]> compute(ChangeBatchConsumer consumer) {
        Map<RowIdentity, DedupStoredRow> rowsByIdentity = new LinkedHashMap<>();
        Map<RowKey, NavigableMap<Long, RowIdentity>> membersByKey = new LinkedHashMap<>();
        Map<RowIdentity, Object[]> emitted = new LinkedHashMap<>();
        long[] snapshotSequence = {0L};

        drainInput(
            input,
            checkpointRows,
            batch -> {
              for (RowChange change : batch.changes()) {
                DedupStoredRow previous = rowsByIdentity.remove(change.identity());
                if (previous != null) {
                  removeDedupMember(membersByKey, previous.key, change.identity());
                }
                if (change.after() != null) {
                  RowKey key = RowKey.from(change.after(), keyOrdinals);
                  rowsByIdentity.put(
                      change.identity(), new DedupStoredRow(key, change.after().clone()));
                  membersByKey
                      .computeIfAbsent(key, ignored -> new TreeMap<>())
                      .put(change.identity().ordinal(), change.identity());
                }
              }

              Map<RowIdentity, Object[]> current =
                  dedupAcceptedRows(rowsByIdentity, membersByKey, allowedDuplication, keepEmpty);
              List<RowChange> outputChanges = diffRows(emitted, current);
              emitted.clear();
              emitted.putAll(current);
              List<Object[]> snapshot = current.values().stream().map(Object[]::clone).toList();
              if (!batch.terminal() && !snapshot.isEmpty() && !outputChanges.isEmpty()) {
                ProgressiveQueryContext.publishOperatorSnapshot(
                    new OperatorSnapshot(
                        operatorIdentity,
                        "CalciteEnumerableIncrementalDedup",
                        outputType,
                        snapshot,
                        batch.sourceRowsProcessed(),
                        rowsByIdentity.size(),
                        ++snapshotSequence[0]));
              }
              consumer.accept(
                  new ChangeBatch(outputChanges, batch.sourceRowsProcessed(), batch.terminal()));
            });
        return emitted.values().stream().map(Object[]::clone).toList();
      }
    };
  }

  static Enumerable<Object[]> topK(
      Object operatorIdentity,
      Enumerable<Object[]> input,
      RelDataType outputType,
      Function1<Object[], Object> keySelector,
      Comparator<Object> keyComparator,
      int offset,
      int fetch) {
    return topK(
        operatorIdentity,
        input,
        outputType,
        keySelector,
        keyComparator,
        offset,
        fetch,
        checkpointRows());
  }

  private static void removeDedupMember(
      Map<RowKey, NavigableMap<Long, RowIdentity>> membersByKey, RowKey key, RowIdentity identity) {
    NavigableMap<Long, RowIdentity> members = membersByKey.get(key);
    if (members == null || members.remove(identity.ordinal()) == null) {
      throw new IllegalStateException("Dedup retraction references an unknown row");
    }
    if (members.isEmpty()) {
      membersByKey.remove(key);
    }
  }

  private static Map<RowIdentity, Object[]> dedupAcceptedRows(
      Map<RowIdentity, DedupStoredRow> rowsByIdentity,
      Map<RowKey, NavigableMap<Long, RowIdentity>> membersByKey,
      int allowedDuplication,
      boolean keepEmpty) {
    List<RowIdentity> accepted = new ArrayList<>();
    for (Map.Entry<RowKey, NavigableMap<Long, RowIdentity>> entry : membersByKey.entrySet()) {
      boolean containsNull = entry.getKey().values.stream().anyMatch(Objects::isNull);
      if (containsNull && !keepEmpty) {
        continue;
      }
      int limit = containsNull ? Integer.MAX_VALUE : allowedDuplication;
      int count = 0;
      for (RowIdentity identity : entry.getValue().values()) {
        if (count++ >= limit) {
          break;
        }
        accepted.add(identity);
      }
    }
    accepted.sort(Comparator.comparingLong(RowIdentity::ordinal));
    Map<RowIdentity, Object[]> rows = new LinkedHashMap<>();
    for (RowIdentity identity : accepted) {
      rows.put(identity, rowsByIdentity.get(identity).row);
    }
    return rows;
  }

  static Enumerable<Object[]> topK(
      Object operatorIdentity,
      Enumerable<Object[]> input,
      RelDataType outputType,
      Function1<Object[], Object> keySelector,
      Comparator<Object> keyComparator,
      int offset,
      int fetch,
      long checkpointRows) {
    return new ChangeEnumerable() {
      @Override
      protected List<Object[]> compute(ChangeBatchConsumer consumer) {
        boolean mutableInput = input instanceof ChangeEnumerable;
        int retainedEntryLimit = saturatedAdd(offset, fetch);
        Comparator<TopKEntry> ordering =
            (left, right) -> {
              int compared = compareKeys(left.key, right.key, keyComparator);
              return compared != 0
                  ? compared
                  : Long.compare(left.identity.ordinal(), right.identity.ordinal());
            };
        Map<RowIdentity, TopKEntry> entriesByIdentity = mutableInput ? new HashMap<>() : Map.of();
        TreeSet<TopKEntry> orderedEntries = new TreeSet<>(ordering);
        Map<RowIdentity, Object[]> emitted = new LinkedHashMap<>();
        long[] snapshotSequence = {0L};

        drainInput(
            input,
            checkpointRows,
            batch -> {
              for (RowChange change : batch.changes()) {
                if (mutableInput) {
                  TopKEntry previous = entriesByIdentity.remove(change.identity());
                  if (previous != null) {
                    orderedEntries.remove(previous);
                  }
                  if (change.after() != null) {
                    TopKEntry replacement =
                        new TopKEntry(
                            change.identity(),
                            change.after().clone(),
                            keySelector.apply(change.after()));
                    entriesByIdentity.put(change.identity(), replacement);
                    orderedEntries.add(replacement);
                  }
                } else {
                  if (change.before() != null || change.after() == null) {
                    throw new IllegalStateException(
                        "Append-only TopK received a mutable row change");
                  }
                  orderedEntries.add(
                      new TopKEntry(
                          change.identity(),
                          change.after().clone(),
                          keySelector.apply(change.after())));
                  if (orderedEntries.size() > retainedEntryLimit) {
                    orderedEntries.pollLast();
                  }
                }
              }

              List<TopKEntry> selected = topKEntries(orderedEntries, offset, fetch);
              Map<RowIdentity, Object[]> current = new LinkedHashMap<>();
              for (TopKEntry entry : selected) {
                current.put(entry.identity, entry.row);
              }
              List<RowChange> outputChanges = diffRows(emitted, current);
              emitted.clear();
              emitted.putAll(current);

              List<Object[]> snapshot = selected.stream().map(entry -> entry.row.clone()).toList();
              if (!batch.terminal() && !snapshot.isEmpty() && !outputChanges.isEmpty()) {
                ProgressiveQueryContext.publishOperatorSnapshot(
                    new OperatorSnapshot(
                        operatorIdentity,
                        "CalciteEnumerableIncrementalTopK",
                        outputType,
                        snapshot,
                        batch.sourceRowsProcessed(),
                        mutableInput ? entriesByIdentity.size() : batch.sourceRowsProcessed(),
                        ++snapshotSequence[0]));
              }
              consumer.accept(
                  new ChangeBatch(outputChanges, batch.sourceRowsProcessed(), batch.terminal()));
            });
        return topKEntries(orderedEntries, offset, fetch).stream()
            .map(entry -> entry.row.clone())
            .toList();
      }
    };
  }

  static boolean supports(List<AggregateCall> calls, int inputFieldCount) {
    if (calls.isEmpty()) {
      return false;
    }
    for (AggregateCall call : calls) {
      if (call.isDistinct()
          || call.hasFilter()
          || call.ignoreNulls()
          || call.distinctKeys != null
          || !call.getCollation().getFieldCollations().isEmpty()
          || call.getArgList().stream().anyMatch(index -> index < 0 || index >= inputFieldCount)
          || !IncrementalAggregateStateFactory.supports(call)) {
        return false;
      }
    }
    return true;
  }

  private static long checkpointRows() {
    return Math.max(
        1L,
        Long.getLong(
            "plugins.ppl.async.incremental_operator.checkpoint_rows", DEFAULT_CHECKPOINT_ROWS));
  }

  private static long nextCheckpoint(long previous, long consumed, long minimumIncrement) {
    if (previous >= Long.MAX_VALUE / 2L) {
      return Long.MAX_VALUE;
    }
    return Math.max(previous * 2L, consumed + minimumIncrement);
  }

  private static int saturatedAdd(int left, int right) {
    long result = (long) left + right;
    return result >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) result;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static int compareKeys(Object left, Object right, Comparator<Object> comparator) {
    if (comparator != null) {
      return comparator.compare(left, right);
    }
    return ((Comparable) left).compareTo(right);
  }

  private static List<TopKEntry> topKEntries(
      NavigableSet<TopKEntry> entries, int offset, int fetch) {
    int from = Math.min(offset, entries.size());
    int to = Math.min(entries.size(), saturatedAdd(from, fetch));
    List<TopKEntry> selected = new ArrayList<>(to - from);
    int index = 0;
    for (TopKEntry entry : entries) {
      if (index >= from && index < to) {
        selected.add(entry);
      }
      if (++index >= to) {
        break;
      }
    }
    return selected;
  }

  private static List<RowChange> diffRows(
      Map<RowIdentity, Object[]> beforeRows, Map<RowIdentity, Object[]> afterRows) {
    Set<RowIdentity> identities = new LinkedHashSet<>(beforeRows.keySet());
    identities.addAll(afterRows.keySet());
    List<RowChange> changes = new ArrayList<>(identities.size());
    for (RowIdentity identity : identities) {
      Object[] before = beforeRows.get(identity);
      Object[] after = afterRows.get(identity);
      if (!Arrays.deepEquals(before, after)) {
        changes.add(new RowChange(identity, before, after));
      }
    }
    return changes;
  }

  private static IncrementalAggregateState[] createStates(List<AggregateCall> calls) {
    IncrementalAggregateState[] states = new IncrementalAggregateState[calls.size()];
    for (int i = 0; i < calls.size(); i++) {
      states[i] = IncrementalAggregateStateFactory.create(calls.get(i));
    }
    return states;
  }

  private static IncrementalAggregateState[] copyStates(IncrementalAggregateState[] source) {
    IncrementalAggregateState[] copied = new IncrementalAggregateState[source.length];
    for (int i = 0; i < source.length; i++) {
      copied[i] = source[i].copy();
    }
    return copied;
  }

  private static void add(
      IncrementalAggregateState[] states, List<AggregateCall> calls, Object[] inputRow) {
    for (int i = 0; i < calls.size(); i++) {
      AggregateCall call = calls.get(i);
      Object value =
          call.getArgList().isEmpty() ? Boolean.TRUE : inputRow[call.getArgList().getFirst()];
      states[i].add(value);
    }
  }

  private static void remove(
      IncrementalAggregateState[] states, List<AggregateCall> calls, Object[] inputRow) {
    for (int i = 0; i < calls.size(); i++) {
      AggregateCall call = calls.get(i);
      Object value =
          call.getArgList().isEmpty() ? Boolean.TRUE : inputRow[call.getArgList().getFirst()];
      states[i].remove(value);
    }
  }

  private static void addAggregateInput(
      LinkedHashMap<RowKey, AggregateGroupState> groups,
      Set<RowKey> dirtyGroups,
      List<Integer> groupOrdinals,
      List<AggregateCall> calls,
      Object[] row,
      long[] nextOutputOrdinal) {
    RowKey key = RowKey.from(row, groupOrdinals);
    AggregateGroupState group =
        groups.computeIfAbsent(
            key,
            ignored ->
                new AggregateGroupState(
                    new RowIdentity(key, nextOutputOrdinal[0]++), createStates(calls)));
    add(group.states, calls, row);
    group.inputCount++;
    dirtyGroups.add(key);
  }

  private static void removeAggregateInput(
      LinkedHashMap<RowKey, AggregateGroupState> groups,
      Set<RowKey> dirtyGroups,
      List<Integer> groupOrdinals,
      List<AggregateCall> calls,
      Object[] row) {
    RowKey key = RowKey.from(row, groupOrdinals);
    AggregateGroupState group = groups.get(key);
    if (group == null || group.inputCount == 0L) {
      throw new IllegalStateException("Aggregate retraction references an unknown input group");
    }
    remove(group.states, calls, row);
    group.inputCount--;
    dirtyGroups.add(key);
    if (group.inputCount == 0L && !groupOrdinals.isEmpty()) {
      groups.remove(key);
    }
  }

  private static List<Object[]> aggregateRows(
      LinkedHashMap<RowKey, AggregateGroupState> groups, List<AggregateCall> calls) {
    List<Object[]> rows = new ArrayList<>(Math.max(1, groups.size()));
    if (groups.isEmpty()) {
      return rows;
    }
    groups.forEach((key, group) -> rows.add(aggregateRow(key, group.states, calls)));
    return rows;
  }

  private static Object[] aggregateRow(
      RowKey key, IncrementalAggregateState[] states, List<AggregateCall> calls) {
    Object[] output = new Object[key.values.size() + calls.size()];
    for (int i = 0; i < key.values.size(); i++) {
      output[i] = key.values.get(i);
    }
    for (int i = 0; i < states.length; i++) {
      output[key.values.size() + i] = states[i].result();
    }
    return output;
  }

  private static final class AggregateGroupState {
    private final RowIdentity identity;
    private final IncrementalAggregateState[] states;
    private long inputCount;

    private AggregateGroupState(RowIdentity identity, IncrementalAggregateState[] states) {
      this.identity = identity;
      this.states = states;
    }
  }

  private static Map<RowIdentity, Object[]> eventStatsRows(
      Map<RowIdentity, EventStatsStoredRow> rowsByIdentity,
      Map<RowKey, EventStatsPartition> partitions,
      List<AggregateCall> calls) {
    List<Map.Entry<RowKey, EventStatsPartition>> orderedPartitions =
        new ArrayList<>(partitions.entrySet());
    orderedPartitions.sort(Comparator.comparingLong(entry -> entry.getValue().rows.firstKey()));
    Map<RowIdentity, Object[]> output = new LinkedHashMap<>(rowsByIdentity.size());
    for (Map.Entry<RowKey, EventStatsPartition> entry : orderedPartitions) {
      EventStatsPartition partition = entry.getValue();
      for (RowIdentity identity : partition.rows.values()) {
        EventStatsStoredRow stored = rowsByIdentity.get(identity);
        if (stored == null) {
          throw new IllegalStateException("Eventstats partition contains an unknown row");
        }
        Object[] row = Arrays.copyOf(stored.row, stored.row.length + calls.size());
        for (int i = 0; i < partition.states.length; i++) {
          row[stored.row.length + i] = partition.states[i].result();
        }
        output.put(identity, row);
      }
    }
    return output;
  }

  private static final class EventStatsPartition {
    private final IncrementalAggregateState[] states;
    private final NavigableMap<Long, RowIdentity> rows = new TreeMap<>();

    private EventStatsPartition(IncrementalAggregateState[] states) {
      this.states = states;
    }
  }

  private record EventStatsStoredRow(RowKey partitionKey, Object[] row) {}

  private static void recomputeRunningWindowSuffix(
      RunningWindowPartition partition,
      long firstDirtyOrdinal,
      Map<RowIdentity, RunningWindowStoredRow> rowsByIdentity,
      List<AggregateCall> calls) {
    Map.Entry<Long, RunningWindowStoredRow> previous = partition.rows.lowerEntry(firstDirtyOrdinal);
    IncrementalAggregateState[] states =
        previous == null ? createStates(calls) : copyStates(previous.getValue().stateAfter);
    for (Map.Entry<Long, RunningWindowStoredRow> entry :
        partition.rows.tailMap(firstDirtyOrdinal, true).entrySet()) {
      RunningWindowStoredRow stored = entry.getValue();
      add(states, calls, stored.inputRow);
      Object[] output = Arrays.copyOf(stored.inputRow, stored.inputRow.length + calls.size());
      for (int i = 0; i < states.length; i++) {
        output[stored.inputRow.length + i] = states[i].result();
      }
      RunningWindowStoredRow recomputed =
          new RunningWindowStoredRow(
              stored.identity, stored.partitionKey, stored.inputRow, output, copyStates(states));
      entry.setValue(recomputed);
      rowsByIdentity.put(stored.identity, recomputed);
    }
  }

  private static Map<RowIdentity, Object[]> runningWindowRows(
      Map<RowIdentity, RunningWindowStoredRow> rowsByIdentity) {
    List<Map.Entry<RowIdentity, RunningWindowStoredRow>> orderedRows =
        new ArrayList<>(rowsByIdentity.entrySet());
    orderedRows.sort(Comparator.comparingLong(entry -> entry.getKey().ordinal()));
    Map<RowIdentity, Object[]> output = new LinkedHashMap<>(orderedRows.size());
    for (Map.Entry<RowIdentity, RunningWindowStoredRow> entry : orderedRows) {
      if (entry.getValue().outputRow == null) {
        throw new IllegalStateException("Running window row was not recomputed");
      }
      output.put(entry.getKey(), entry.getValue().outputRow);
    }
    return output;
  }

  private static final class RunningWindowPartition {
    private final NavigableMap<Long, RunningWindowStoredRow> rows = new TreeMap<>();
  }

  private record RunningWindowStoredRow(
      RowIdentity identity,
      RowKey partitionKey,
      Object[] inputRow,
      Object[] outputRow,
      IncrementalAggregateState[] stateAfter) {}

  private record RowKey(List<Object> values) {
    private static RowKey from(Object[] row, List<Integer> ordinals) {
      List<Object> values = new ArrayList<>(ordinals.size());
      for (Integer ordinal : ordinals) {
        values.add(row[ordinal]);
      }
      return new RowKey(Collections.unmodifiableList(values));
    }
  }

  private interface IncrementalAggregateState {
    void add(Object value);

    void remove(Object value);

    Object result();

    IncrementalAggregateState copy();
  }

  private static final class IncrementalAggregateStateFactory {
    private IncrementalAggregateStateFactory() {}

    private static boolean supports(AggregateCall call) {
      SqlKind kind = call.getAggregation().getKind();
      int argumentCount = call.getArgList().size();
      if (kind == SqlKind.COUNT) {
        return argumentCount <= 1;
      }
      if (kind == SqlKind.ROW_NUMBER) {
        return argumentCount == 0;
      }
      if (argumentCount != 1) {
        return false;
      }
      return switch (kind) {
        case AVG -> isFloatingPoint(call.getType().getSqlTypeName());
        case MIN, MAX -> true;
        case SUM, SUM0 -> isNumeric(call.getType().getSqlTypeName());
        default -> false;
      };
    }

    private static IncrementalAggregateState create(AggregateCall call) {
      return switch (call.getAggregation().getKind()) {
        case COUNT -> new CountState();
        case ROW_NUMBER -> new RowNumberState();
        case AVG -> new AverageState(call.getType().getSqlTypeName());
        case MIN -> new MinMaxState(true);
        case MAX -> new MinMaxState(false);
        case SUM, SUM0 -> new SumState(call);
        default ->
            throw new IllegalArgumentException(
                "Unsupported incremental aggregation " + call.getAggregation());
      };
    }

    private static boolean isFloatingPoint(SqlTypeName type) {
      return type == SqlTypeName.FLOAT || type == SqlTypeName.REAL || type == SqlTypeName.DOUBLE;
    }

    private static boolean isNumeric(SqlTypeName type) {
      return SqlTypeName.NUMERIC_TYPES.contains(type);
    }
  }

  private static final class RowNumberState implements IncrementalAggregateState {
    private long rowNumber;

    @Override
    public void add(Object value) {
      rowNumber++;
    }

    @Override
    public void remove(Object value) {
      if (rowNumber == 0L) {
        throw new IllegalStateException("ROW_NUMBER state underflow");
      }
      rowNumber--;
    }

    @Override
    public Object result() {
      return rowNumber;
    }

    @Override
    public IncrementalAggregateState copy() {
      RowNumberState copied = new RowNumberState();
      copied.rowNumber = rowNumber;
      return copied;
    }
  }

  private static final class CountState implements IncrementalAggregateState {
    private long count;

    @Override
    public void add(Object value) {
      if (value != null) {
        count++;
      }
    }

    @Override
    public void remove(Object value) {
      if (value != null) {
        if (count == 0L) {
          throw new IllegalStateException("COUNT state underflow");
        }
        count--;
      }
    }

    @Override
    public Object result() {
      return count;
    }

    @Override
    public IncrementalAggregateState copy() {
      CountState copied = new CountState();
      copied.count = count;
      return copied;
    }
  }

  /**
   * PPL integral AVG uses {@code BigintAvgAggFunction}, whose accumulator is a double. Keeping the
   * same update order here preserves its overflow and rounding behavior.
   */
  private static final class AverageState implements IncrementalAggregateState {
    private final SqlTypeName outputType;
    private double sum;
    private long count;

    private AverageState(SqlTypeName outputType) {
      this.outputType = outputType;
    }

    @Override
    public void add(Object value) {
      if (value instanceof Number number) {
        sum += number.doubleValue();
        count++;
      }
    }

    @Override
    public void remove(Object value) {
      if (value instanceof Number number) {
        if (count == 0L) {
          throw new IllegalStateException("AVG state underflow");
        }
        sum -= number.doubleValue();
        count--;
      }
    }

    @Override
    public Object result() {
      if (count == 0L) {
        return null;
      }
      double average = sum / count;
      return outputType == SqlTypeName.FLOAT || outputType == SqlTypeName.REAL
          ? (float) average
          : average;
    }

    @Override
    public IncrementalAggregateState copy() {
      AverageState copied = new AverageState(outputType);
      copied.sum = sum;
      copied.count = count;
      return copied;
    }
  }

  private static final class MinMaxState implements IncrementalAggregateState {
    private final boolean minimum;
    private final NavigableMap<Object, Integer> values =
        new TreeMap<>(IncrementalEnumerableOperators::compareValues);

    private MinMaxState(boolean minimum) {
      this.minimum = minimum;
    }

    @Override
    public void add(Object candidate) {
      if (candidate == null) {
        return;
      }
      values.merge(candidate, 1, Integer::sum);
    }

    @Override
    public void remove(Object candidate) {
      if (candidate == null) {
        return;
      }
      Integer count = values.get(candidate);
      if (count == null) {
        throw new IllegalStateException("MIN/MAX state retraction references an unknown value");
      }
      if (count == 1) {
        values.remove(candidate);
      } else {
        values.put(candidate, count - 1);
      }
    }

    @Override
    public Object result() {
      if (values.isEmpty()) {
        return null;
      }
      return minimum ? values.firstKey() : values.lastKey();
    }

    @Override
    public IncrementalAggregateState copy() {
      MinMaxState copied = new MinMaxState(minimum);
      copied.values.putAll(values);
      return copied;
    }
  }

  private static final class SumState implements IncrementalAggregateState {
    private final SqlTypeName outputType;
    private final boolean zeroOnEmpty;
    private final boolean checkedLong;
    private long valueCount;
    private long longSum;
    private float floatSum;
    private double doubleSum;
    private BigDecimal decimalSum = BigDecimal.ZERO;

    private SumState(AggregateCall call) {
      outputType = call.getType().getSqlTypeName();
      zeroOnEmpty = call.getAggregation().getKind() == SqlKind.SUM0;
      checkedLong = call.getAggregation() == PPLBuiltinOperators.CHECKED_LONG_SUM;
    }

    @Override
    public void add(Object value) {
      if (!(value instanceof Number number)) {
        return;
      }
      valueCount++;
      switch (outputType) {
        case TINYINT, SMALLINT, INTEGER, BIGINT ->
            longSum =
                checkedLong
                    ? Math.addExact(longSum, number.longValue())
                    : longSum + number.longValue();
        case FLOAT, REAL -> floatSum += number.floatValue();
        case DOUBLE -> doubleSum += number.doubleValue();
        case DECIMAL ->
            decimalSum =
                decimalSum.add(
                    value instanceof BigDecimal decimal
                        ? decimal
                        : new BigDecimal(number.toString()));
        default -> throw new IllegalStateException("Unsupported SUM output type " + outputType);
      }
    }

    @Override
    public void remove(Object value) {
      if (!(value instanceof Number number)) {
        return;
      }
      if (valueCount == 0L) {
        throw new IllegalStateException("SUM state underflow");
      }
      valueCount--;
      switch (outputType) {
        case TINYINT, SMALLINT, INTEGER, BIGINT ->
            longSum =
                checkedLong
                    ? Math.subtractExact(longSum, number.longValue())
                    : longSum - number.longValue();
        case FLOAT, REAL -> floatSum -= number.floatValue();
        case DOUBLE -> doubleSum -= number.doubleValue();
        case DECIMAL ->
            decimalSum =
                decimalSum.subtract(
                    value instanceof BigDecimal decimal
                        ? decimal
                        : new BigDecimal(number.toString()));
        default -> throw new IllegalStateException("Unsupported SUM output type " + outputType);
      }
      // The aggregate group tracks row membership separately. A zero sum is a valid result after
      // retractions; the group disappears only when its input count reaches zero.
    }

    @Override
    public Object result() {
      if (valueCount == 0L && !zeroOnEmpty) {
        return null;
      }
      return switch (outputType) {
        case TINYINT -> (byte) longSum;
        case SMALLINT -> (short) longSum;
        case INTEGER -> (int) longSum;
        case BIGINT -> longSum;
        case FLOAT, REAL -> floatSum;
        case DOUBLE -> doubleSum;
        case DECIMAL -> decimalSum;
        default -> throw new IllegalStateException("Unsupported SUM output type " + outputType);
      };
    }

    @Override
    public IncrementalAggregateState copy() {
      SumState copied = new SumState(outputType, zeroOnEmpty, checkedLong);
      copied.valueCount = valueCount;
      copied.longSum = longSum;
      copied.floatSum = floatSum;
      copied.doubleSum = doubleSum;
      copied.decimalSum = decimalSum;
      return copied;
    }

    private SumState(SqlTypeName outputType, boolean zeroOnEmpty, boolean checkedLong) {
      this.outputType = outputType;
      this.zeroOnEmpty = zeroOnEmpty;
      this.checkedLong = checkedLong;
    }
  }

  private static void drainInput(
      Enumerable<Object[]> input, long checkpointRows, ChangeBatchConsumer consumer) {
    if (input instanceof ChangeEnumerable changeInput) {
      changeInput.drain(consumer);
      return;
    }
    List<RowChange> pending = new ArrayList<>();
    long consumed = 0L;
    long nextCheckpoint = checkpointRows;
    try (Enumerator<Object[]> rows = input.enumerator()) {
      while (rows.moveNext()) {
        Object[] row = rows.current().clone();
        RowIdentity identity = new RowIdentity(new SourceRowKey(consumed), consumed);
        pending.add(new RowChange(identity, null, row));
        consumed++;
        if (consumed >= nextCheckpoint) {
          consumer.accept(new ChangeBatch(pending, consumed, false));
          pending = new ArrayList<>();
          nextCheckpoint = nextCheckpoint(nextCheckpoint, consumed, checkpointRows);
        }
      }
    }
    consumer.accept(new ChangeBatch(pending, consumed, true));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static int compareValues(Object left, Object right) {
    return ((Comparable) left).compareTo(right);
  }

  private record SourceRowKey(long ordinal) {}

  private record RowIdentity(Object key, long ordinal) {
    private RowIdentity {
      Objects.requireNonNull(key);
    }
  }

  private record RowChange(RowIdentity identity, Object[] before, Object[] after) {
    private RowChange {
      Objects.requireNonNull(identity);
      before = before == null ? null : before.clone();
      after = after == null ? null : after.clone();
      if (before == null && after == null) {
        throw new IllegalArgumentException("A row change must contain before or after");
      }
    }
  }

  private record ChangeBatch(List<RowChange> changes, long sourceRowsProcessed, boolean terminal) {
    private ChangeBatch {
      changes = List.copyOf(changes);
      if (sourceRowsProcessed < 0L) {
        throw new IllegalArgumentException("sourceRowsProcessed must be non-negative");
      }
    }
  }

  @FunctionalInterface
  private interface ChangeBatchConsumer {
    void accept(ChangeBatch batch);
  }

  /**
   * One-shot finite changelog plus a cached final Enumerable view.
   *
   * <p>This mirrors Spark's update-mode stateful execution at a finite-query scope: every child
   * batch carries keyed before/after rows, while JDBC and existing Calcite parents still see only
   * the final append-only result. A parent incremental operator drains the changelog directly;
   * ordinary parents trigger the same computation through {@link #enumerator()}.
   */
  private abstract static class ChangeEnumerable extends AbstractEnumerable<Object[]> {
    private List<Object[]> finalRows;
    private boolean computing;

    protected abstract List<Object[]> compute(ChangeBatchConsumer consumer);

    private synchronized List<Object[]> drain(ChangeBatchConsumer consumer) {
      if (finalRows != null) {
        return finalRows;
      }
      if (computing) {
        throw new IllegalStateException("Recursive incremental enumerable evaluation");
      }
      computing = true;
      try {
        finalRows = List.copyOf(compute(consumer));
        return finalRows;
      } finally {
        computing = false;
      }
    }

    @Override
    public Enumerator<Object[]> enumerator() {
      return new Enumerator<>() {
        private List<Object[]> rows;
        private int index = -1;

        @Override
        public Object[] current() {
          return rows.get(index);
        }

        @Override
        public boolean moveNext() {
          if (rows == null) {
            rows = drain(ignored -> {});
          }
          return ++index < rows.size();
        }

        @Override
        public void reset() {
          index = -1;
        }

        @Override
        public void close() {}
      };
    }
  }

  private record TopKEntry(RowIdentity identity, Object[] row, Object key) {}

  private record DedupStoredRow(RowKey key, Object[] row) {}

  @FunctionalInterface
  private interface Materializer {
    List<Object[]> materialize();
  }

  private static final class MaterializingEnumerable extends AbstractEnumerable<Object[]> {
    private final Materializer materializer;

    private MaterializingEnumerable(Materializer materializer) {
      this.materializer = materializer;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
      return new Enumerator<>() {
        private List<Object[]> rows;
        private int index = -1;

        @Override
        public Object[] current() {
          return rows.get(index);
        }

        @Override
        public boolean moveNext() {
          if (rows == null) {
            rows = materializer.materialize();
          }
          return ++index < rows.size();
        }

        @Override
        public void reset() {
          index = -1;
        }

        @Override
        public void close() {}
      };
    }
  }
}
