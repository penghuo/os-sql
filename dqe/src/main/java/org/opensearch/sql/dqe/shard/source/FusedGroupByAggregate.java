/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.sql.dqe.operator.LongOpenHashSet;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

/**
 * Fused scan-group-aggregate operator that uses Lucene SortedSetDocValues ordinals as hash keys
 * during GROUP BY aggregation on string columns. This avoids the expensive lookupOrd() call for
 * every row, deferring string resolution to the final output phase.
 *
 * <p>The key optimization: during aggregation, string GROUP BY keys are represented as long
 * ordinals from SortedSetDocValues. Since ordinals are segment-local, we aggregate per-segment
 * using ordinals, then merge results across segments by resolving ordinals to strings.
 *
 * <p>Supports GROUP BY patterns with:
 *
 * <ul>
 *   <li>Single VARCHAR key (e.g., GROUP BY SearchPhrase)
 *   <li>Multiple keys mixing VARCHAR and numeric types (e.g., GROUP BY SearchEngineID,
 *       SearchPhrase)
 * </ul>
 *
 * <p>Aggregate functions: COUNT(*), SUM, MIN, MAX, AVG, COUNT(DISTINCT).
 */
public final class FusedGroupByAggregate {

  private static final Pattern AGG_FUNCTION =
      Pattern.compile(
          "^\\s*(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)\\s*$", Pattern.CASE_INSENSITIVE);

  /**
   * Pattern to recognize DATE_TRUNC expressions in group-by keys. Matches expressions like
   * "date_trunc('minute', EventTime)" and extracts the unit and source column name.
   */
  private static final Pattern DATE_TRUNC_PATTERN =
      Pattern.compile(
          "^date_trunc\\('(second|minute|hour|day|month|year)',\\s*(\\w+)\\)$",
          Pattern.CASE_INSENSITIVE);

  private FusedGroupByAggregate() {}

  /**
   * Find the TableScanNode underneath an AggregationNode, looking through an optional EvalNode.
   * Returns null if the child structure is not TableScanNode or EvalNode → TableScanNode.
   */
  private static TableScanNode findChildTableScan(AggregationNode aggNode) {
    DqePlanNode child = aggNode.getChild();
    if (child instanceof TableScanNode tsn) {
      return tsn;
    }
    if (child instanceof EvalNode evalNode && evalNode.getChild() instanceof TableScanNode tsn) {
      return tsn;
    }
    return null;
  }

  /**
   * Check if the shard plan is a GROUP BY aggregation that can use the fused path. Requirements:
   *
   * <ul>
   *   <li>Plan is AggregationNode with non-empty groupByKeys
   *   <li>Child is a TableScanNode or EvalNode → TableScanNode
   *   <li>All group-by keys are either plain columns with VARCHAR/numeric/timestamp types, or
   *       supported expressions like DATE_TRUNC on a timestamp column
   *   <li>All aggregate functions are supported
   * </ul>
   */
  public static boolean canFuse(AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    if (aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    if (findChildTableScan(aggNode) == null) {
      return false;
    }

    // Check all group-by keys are supported types (VARCHAR, numeric, or timestamp)
    // or recognized expression patterns (DATE_TRUNC)
    for (String key : aggNode.getGroupByKeys()) {
      Type type = columnTypeMap.get(key);
      if (type != null) {
        // Plain column reference
        if (!(type instanceof VarcharType) && !isNumericOrTimestamp(type)) {
          return false; // Unsupported group-by key type
        }
      } else {
        // Not a plain column — check if it's a supported expression
        Matcher dtm = DATE_TRUNC_PATTERN.matcher(key);
        if (dtm.matches()) {
          // Verify the source column is a timestamp type
          String sourceCol = dtm.group(2);
          Type sourceType = columnTypeMap.get(sourceCol);
          if (!(sourceType instanceof TimestampType)) {
            return false;
          }
        } else {
          return false; // Unknown expression, can't fuse
        }
      }
    }

    // Check all aggregate functions are supported
    for (String func : aggNode.getAggregateFunctions()) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        return false;
      }
    }
    return true;
  }

  /** Check if a type is a numeric or timestamp type suitable for group-by keys. */
  private static boolean isNumericOrTimestamp(Type type) {
    return type instanceof BigintType
        || type instanceof IntegerType
        || type instanceof SmallintType
        || type instanceof TinyintType
        || type instanceof DoubleType
        || type instanceof TimestampType
        || type instanceof BooleanType;
  }

  /**
   * Execute the fused GROUP BY aggregation directly from Lucene DocValues. For keys that include
   * VARCHAR columns, uses SortedSetDocValues ordinals as hash keys during per-segment aggregation,
   * then resolves ordinals to strings across segments. For numeric-only keys, aggregates directly
   * into a global map without ordinal resolution.
   *
   * @param aggNode the aggregation plan node
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @param columnTypeMap type mapping for columns
   * @return a list containing a single Page with the aggregated results
   */
  public static List<Page> execute(
      AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
      throws Exception {
    TableScanNode scanNode = findChildTableScan(aggNode);
    List<String> groupByKeys = aggNode.getGroupByKeys();
    List<String> aggFunctions = aggNode.getAggregateFunctions();

    // Parse aggregate specs
    List<AggSpec> specs = new ArrayList<>();
    for (String func : aggFunctions) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        throw new IllegalArgumentException("Unsupported aggregate: " + func);
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      String arg = m.group(3).trim();
      Type argType = arg.equals("*") ? null : columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
      specs.add(new AggSpec(funcName, isDistinct, arg, argType));
    }

    // Classify group-by keys, detecting DATE_TRUNC expressions
    List<KeyInfo> keyInfos = new ArrayList<>();
    boolean hasVarchar = false;
    for (String key : groupByKeys) {
      Type type = columnTypeMap.get(key);
      if (type != null) {
        // Plain column reference
        boolean isVarchar = type instanceof VarcharType;
        keyInfos.add(new KeyInfo(key, type, isVarchar, null, null));
        if (isVarchar) {
          hasVarchar = true;
        }
      } else {
        // Check for DATE_TRUNC expression
        Matcher dtm = DATE_TRUNC_PATTERN.matcher(key);
        if (dtm.matches()) {
          String unit = dtm.group(1).toLowerCase(Locale.ROOT);
          String sourceCol = dtm.group(2);
          // Output type is TimestampType, source column is read from DocValues
          keyInfos.add(
              new KeyInfo(sourceCol, TimestampType.TIMESTAMP_MILLIS, false, "date_trunc", unit));
        } else {
          throw new IllegalArgumentException("Unsupported group-by expression: " + key);
        }
      }
    }

    // Dispatch to specialized path based on key types
    if (hasVarchar) {
      // Fast path: single VARCHAR key with COUNT(*) only — uses HashMap<String, long> directly,
      // avoiding MergedGroupKey wrapper and AccumulatorGroup object allocation per group.
      if (keyInfos.size() == 1
          && keyInfos.get(0).isVarchar
          && specs.size() == 1
          && "COUNT".equals(specs.get(0).funcName)
          && "*".equals(specs.get(0).arg)) {
        return executeSingleVarcharCountStar(
            shard, query, keyInfos.get(0).name, columnTypeMap, groupByKeys);
      }
      return executeWithVarcharKeys(shard, query, keyInfos, specs, columnTypeMap, groupByKeys);
    } else {
      return executeNumericOnly(shard, query, keyInfos, specs, columnTypeMap, groupByKeys);
    }
  }

  /**
   * Apply DATE_TRUNC transformation to an epoch-millis value. Truncates to the specified unit
   * boundary directly in millis, avoiding ZonedDateTime allocation for the common 'minute' case.
   *
   * @param millis epoch milliseconds from DocValues
   * @param unit the truncation unit (second, minute, hour, day, month, year)
   * @return truncated epoch milliseconds
   */
  private static long truncateMillis(long millis, String unit) {
    switch (unit) {
      case "second":
        return millis / 1000 * 1000;
      case "minute":
        return millis / 60_000 * 60_000;
      case "hour":
        return millis / 3_600_000 * 3_600_000;
      case "day":
        return millis / 86_400_000 * 86_400_000;
      case "month":
      case "year":
        // For month/year, fall through to calendar-based truncation
        java.time.Instant instant = java.time.Instant.ofEpochMilli(millis);
        java.time.ZonedDateTime zdt = instant.atZone(java.time.ZoneOffset.UTC);
        if ("month".equals(unit)) {
          zdt = zdt.withDayOfMonth(1).toLocalDate().atStartOfDay(java.time.ZoneOffset.UTC);
        } else {
          zdt =
              zdt.withMonth(1)
                  .withDayOfMonth(1)
                  .toLocalDate()
                  .atStartOfDay(java.time.ZoneOffset.UTC);
        }
        return zdt.toInstant().toEpochMilli();
      default:
        return millis;
    }
  }

  /**
   * Ultra-fast path for single VARCHAR key with COUNT(*). Uses per-segment ordinal arrays for
   * counting. When the index has a single segment (common for small-to-medium indices), the ordinal
   * array IS the final result — ordinals are resolved to strings only when building the output
   * Page, completely avoiding the intermediate HashMap&lt;String, long&gt; and all String
   * allocations during aggregation. For multi-segment indices, falls back to a byte[]-based HashMap
   * that avoids the expensive UTF-8 to Java char[] round-trip.
   */
  private static List<Page> executeSingleVarcharCountStar(
      IndexShard shard,
      Query query,
      String columnName,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys)
      throws Exception {

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-varchar-count")) {

      // Check if we have a single segment — enables the zero-string-allocation fast path
      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();

      if (leaves.size() == 1) {
        // === Single-segment fast path ===
        // Count per-ordinal in a long array, then build output Page directly from ordinals.
        // No HashMap, no String allocation during aggregation.
        // Use 10M ordinal limit (80MB memory) — covers all practical single-segment indices.
        final long[][] ordCountsHolder = new long[1][];
        final SortedSetDocValues[] dvHolder = new SortedSetDocValues[1];
        final boolean[] usedOrdArray = {false};

        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedSetDocValues dv = context.reader().getSortedSetDocValues(columnName);
                dvHolder[0] = dv;
                long ordCount = (dv != null) ? dv.getValueCount() : 0;
                if (ordCount > 0 && ordCount <= 10_000_000) {
                  long[] ordCounts = new long[(int) ordCount];
                  ordCountsHolder[0] = ordCounts;
                  usedOrdArray[0] = true;
                  return new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) throws IOException {
                      if (dv.advanceExact(doc)) {
                        ordCounts[(int) dv.nextOrd()]++;
                      }
                    }
                  };
                }
                // Fallback: won't benefit from single-segment path
                usedOrdArray[0] = false;
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) {}
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });

        if (usedOrdArray[0] && dvHolder[0] != null) {
          SortedSetDocValues dv = dvHolder[0];
          long[] ordCounts = ordCountsHolder[0];

          // Count non-zero entries first to size builders correctly
          int groupCount = 0;
          for (int i = 0; i < ordCounts.length; i++) {
            if (ordCounts[i] > 0) groupCount++;
          }
          if (groupCount == 0) return List.of();

          BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
          BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, groupCount);
          for (int i = 0; i < ordCounts.length; i++) {
            if (ordCounts[i] > 0) {
              BytesRef bytes = dv.lookupOrd(i);
              // wrappedBuffer is safe: writeSlice copies bytes into BlockBuilder's buffer
              VarcharType.VARCHAR.writeSlice(
                  keyBuilder, Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              BigintType.BIGINT.writeLong(countBuilder, ordCounts[i]);
            }
          }
          return List.of(new Page(keyBuilder.build(), countBuilder.build()));
        }
        // Fall through to multi-segment path for very large ordinal spaces
      }

      // === Multi-segment path ===
      // Use HashMap<BytesRefKey, long[]> to avoid String allocation during cross-segment merge.
      HashMap<BytesRefKey, long[]> globalCounts = new HashMap<>();

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              SortedSetDocValues dv = context.reader().getSortedSetDocValues(columnName);
              long ordCount = (dv != null) ? dv.getValueCount() : 0;
              long[] ordCounts =
                  (ordCount > 0 && ordCount <= 1_000_000) ? new long[(int) ordCount] : null;
              HashMap<Long, long[]> ordMap = (ordCounts == null) ? new HashMap<>() : null;

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  if (dv != null && dv.advanceExact(doc)) {
                    long ord = dv.nextOrd();
                    if (ordCounts != null) {
                      ordCounts[(int) ord]++;
                    } else {
                      ordMap.merge(
                          ord,
                          new long[] {1},
                          (a, b) -> {
                            a[0]++;
                            return a;
                          });
                    }
                  }
                }

                @Override
                public void finish() throws IOException {
                  if (dv == null) return;
                  if (ordCounts != null) {
                    for (int i = 0; i < ordCounts.length; i++) {
                      if (ordCounts[i] > 0) {
                        BytesRef bytes = dv.lookupOrd(i);
                        BytesRefKey key = new BytesRefKey(bytes);
                        long[] existing = globalCounts.get(key);
                        if (existing == null) {
                          globalCounts.put(key, new long[] {ordCounts[i]});
                        } else {
                          existing[0] += ordCounts[i];
                        }
                      }
                    }
                  } else {
                    for (Map.Entry<Long, long[]> entry : ordMap.entrySet()) {
                      BytesRef bytes = dv.lookupOrd(entry.getKey());
                      BytesRefKey key = new BytesRefKey(bytes);
                      long[] existing = globalCounts.get(key);
                      if (existing == null) {
                        globalCounts.put(key, new long[] {entry.getValue()[0]});
                      } else {
                        existing[0] += entry.getValue()[0];
                      }
                    }
                  }
                }
              };
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });

      if (globalCounts.isEmpty()) {
        return List.of();
      }

      int groupCount = globalCounts.size();
      BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
      BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, groupCount);

      for (Map.Entry<BytesRefKey, long[]> entry : globalCounts.entrySet()) {
        VarcharType.VARCHAR.writeSlice(keyBuilder, Slices.wrappedBuffer(entry.getKey().bytes));
        BigintType.BIGINT.writeLong(countBuilder, entry.getValue()[0]);
      }

      return List.of(new Page(keyBuilder.build(), countBuilder.build()));
    }
  }

  /**
   * Fast path for numeric-only GROUP BY keys. Since all keys are raw long values (no segment-local
   * ordinals), we aggregate directly into a global map across all segments without
   * per-segment/cross-segment merge overhead. Supports DATE_TRUNC expressions on timestamp columns
   * by applying truncation during key value extraction.
   *
   * <p>Uses a reusable {@link NumericProbeKey} for HashMap lookups to avoid per-doc allocation. An
   * immutable {@link SegmentGroupKey} is only created when inserting a new group (cache miss),
   * eliminating ~4 array allocations per doc for the common case where the group already exists.
   */
  private static List<Page> executeNumericOnly(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys)
      throws Exception {

    // Global map: SegmentGroupKey (long[]) -> AccumulatorGroup
    // For numeric-only keys, long values are globally unique (not ordinals),
    // so we can accumulate directly without per-segment resolution.
    Map<SegmentGroupKey, AccumulatorGroup> globalGroups = new HashMap<>();

    // Pre-compute which keys need DATE_TRUNC transformation and their units
    final String[] truncUnits = new String[keyInfos.size()];
    boolean hasTrunc = false;
    for (int i = 0; i < keyInfos.size(); i++) {
      KeyInfo ki = keyInfos.get(i);
      if ("date_trunc".equals(ki.exprFunc())) {
        truncUnits[i] = ki.exprUnit();
        hasTrunc = true;
      }
    }
    final boolean anyTrunc = hasTrunc;

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric")) {

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              final int numKeys = keyInfos.size();

              // Open doc values for group-by keys (all numeric)
              SortedNumericDocValues[] keyReaders = new SortedNumericDocValues[numKeys];
              for (int i = 0; i < numKeys; i++) {
                keyReaders[i] = context.reader().getSortedNumericDocValues(keyInfos.get(i).name());
              }

              // Open doc values for aggregate arguments
              Object[] aggReaders = new Object[specs.size()];
              for (int i = 0; i < specs.size(); i++) {
                AggSpec spec = specs.get(i);
                if ("*".equals(spec.arg)) {
                  aggReaders[i] = null;
                } else if (spec.argType instanceof VarcharType) {
                  aggReaders[i] = context.reader().getSortedSetDocValues(spec.arg);
                } else {
                  aggReaders[i] = context.reader().getSortedNumericDocValues(spec.arg);
                }
              }

              // Pre-allocate a reusable probe key to avoid per-doc allocation.
              // The probe key is mutated in-place for each doc and used for HashMap.get().
              // An immutable SegmentGroupKey is only created on cache miss (new group).
              final NumericProbeKey probeKey = new NumericProbeKey(numKeys);

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  // Reset and populate the reusable probe key
                  probeKey.reset();
                  for (int k = 0; k < numKeys; k++) {
                    SortedNumericDocValues dv = keyReaders[k];
                    if (dv != null && dv.advanceExact(doc)) {
                      long val = dv.nextValue();
                      if (anyTrunc && truncUnits[k] != null) {
                        val = truncateMillis(val, truncUnits[k]);
                      }
                      probeKey.set(k, val);
                    } else {
                      probeKey.setNull(k);
                    }
                  }
                  probeKey.computeHash();

                  // Fast path: probe with reusable key (no allocation for existing groups)
                  AccumulatorGroup accGroup = globalGroups.get(probeKey);
                  if (accGroup == null) {
                    // Cache miss: create an immutable key copy and new accumulator group
                    SegmentGroupKey immutableKey = probeKey.toImmutableKey();
                    accGroup = createAccumulatorGroup(specs);
                    globalGroups.put(immutableKey, accGroup);
                  }
                  accumulateDoc(doc, accGroup, specs, aggReaders);
                }
              };
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });
    }

    if (globalGroups.isEmpty()) {
      return List.of();
    }

    // Build result Page directly from SegmentGroupKey (no ordinal resolution needed)
    int numGroupKeys = groupByKeys.size();
    int numAggs = specs.size();
    int totalColumns = numGroupKeys + numAggs;
    int groupCount = globalGroups.size();

    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int i = 0; i < numGroupKeys; i++) {
      builders[i] = keyInfos.get(i).type.createBlockBuilder(null, groupCount);
    }
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
    }

    for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry : globalGroups.entrySet()) {
      SegmentGroupKey key = entry.getKey();
      AccumulatorGroup accGroup = entry.getValue();

      // Write group-by keys (all numeric)
      for (int k = 0; k < numGroupKeys; k++) {
        if (key.nulls[k]) {
          builders[k].appendNull();
        } else {
          writeNumericKeyValue(builders[k], keyInfos.get(k), key.values[k]);
        }
      }

      // Write aggregate results
      for (int a = 0; a < numAggs; a++) {
        accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
      }
    }

    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) {
      blocks[i] = builders[i].build();
    }

    return List.of(new Page(blocks));
  }

  /**
   * Path for GROUP BY keys that include at least one VARCHAR column. Uses SortedSetDocValues
   * ordinals as hash keys during per-segment aggregation. For single-segment indices (common), the
   * segment-local ordinals ARE the final result — ordinals are resolved to raw UTF-8 bytes only at
   * Page output time, completely bypassing MergedGroupKey and String allocation. For multi-segment
   * indices, resolves ordinals to BytesRefKey (raw UTF-8) for cross-segment merge, still avoiding
   * the expensive UTF-8 to Java char[] round-trip.
   */
  private static List<Page> executeWithVarcharKeys(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys)
      throws Exception {

    // Pre-compute DATE_TRUNC units for numeric keys in the varchar path
    final String[] truncUnits = new String[keyInfos.size()];
    for (int i = 0; i < keyInfos.size(); i++) {
      KeyInfo ki = keyInfos.get(i);
      if ("date_trunc".equals(ki.exprFunc)) {
        truncUnits[i] = ki.exprUnit;
      }
    }

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby")) {

      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
      boolean singleSegment = (leaves.size() == 1);

      if (singleSegment) {
        // === Single-segment fast path ===
        // Aggregate into SegmentGroupKey map using ordinals, then build Page directly
        // by resolving ordinals to UTF-8 bytes only at output time. No MergedGroupKey,
        // no String allocation, no cross-segment merge overhead.
        Map<SegmentGroupKey, AccumulatorGroup> segmentGroups = new HashMap<>();
        // Hold references to the segment's DocValues for ordinal resolution at output time
        final Object[][] keyReadersHolder = new Object[1][];

        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                Object[] keyReaders = new Object[keyInfos.size()];
                for (int i = 0; i < keyInfos.size(); i++) {
                  KeyInfo ki = keyInfos.get(i);
                  if (ki.isVarchar) {
                    keyReaders[i] = context.reader().getSortedSetDocValues(ki.name);
                  } else {
                    keyReaders[i] = context.reader().getSortedNumericDocValues(ki.name);
                  }
                }
                keyReadersHolder[0] = keyReaders;

                Object[] aggReaders = new Object[specs.size()];
                for (int i = 0; i < specs.size(); i++) {
                  AggSpec spec = specs.get(i);
                  if ("*".equals(spec.arg)) {
                    aggReaders[i] = null;
                  } else if (spec.argType instanceof VarcharType) {
                    aggReaders[i] = context.reader().getSortedSetDocValues(spec.arg);
                  } else {
                    aggReaders[i] = context.reader().getSortedNumericDocValues(spec.arg);
                  }
                }

                final int numKeys = keyInfos.size();
                final NumericProbeKey probeKey = new NumericProbeKey(numKeys);

                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    probeKey.reset();
                    for (int k = 0; k < numKeys; k++) {
                      KeyInfo ki = keyInfos.get(k);
                      if (ki.isVarchar) {
                        SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
                        if (dv != null && dv.advanceExact(doc)) {
                          probeKey.set(k, dv.nextOrd());
                        } else {
                          probeKey.setNull(k);
                        }
                      } else {
                        SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[k];
                        if (dv != null && dv.advanceExact(doc)) {
                          long val = dv.nextValue();
                          if (truncUnits[k] != null) {
                            val = truncateMillis(val, truncUnits[k]);
                          }
                          probeKey.set(k, val);
                        } else {
                          probeKey.setNull(k);
                        }
                      }
                    }
                    probeKey.computeHash();

                    AccumulatorGroup accGroup = segmentGroups.get(probeKey);
                    if (accGroup == null) {
                      SegmentGroupKey immutableKey = probeKey.toImmutableKey();
                      accGroup = createAccumulatorGroup(specs);
                      segmentGroups.put(immutableKey, accGroup);
                    }
                    accumulateDoc(doc, accGroup, specs, aggReaders);
                  }
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });

        if (segmentGroups.isEmpty()) {
          return List.of();
        }

        // Build output Page directly from segment ordinals — no String allocation
        Object[] keyReaders = keyReadersHolder[0];
        int numGroupKeys = groupByKeys.size();
        int numAggs = specs.size();
        int totalColumns = numGroupKeys + numAggs;
        int groupCount = segmentGroups.size();

        BlockBuilder[] builders = new BlockBuilder[totalColumns];
        for (int i = 0; i < numGroupKeys; i++) {
          builders[i] = keyInfos.get(i).type.createBlockBuilder(null, groupCount);
        }
        for (int i = 0; i < numAggs; i++) {
          builders[numGroupKeys + i] =
              resolveAggOutputType(specs.get(i), columnTypeMap)
                  .createBlockBuilder(null, groupCount);
        }

        for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry : segmentGroups.entrySet()) {
          SegmentGroupKey sgk = entry.getKey();
          AccumulatorGroup accGroup = entry.getValue();

          for (int k = 0; k < numGroupKeys; k++) {
            if (sgk.nulls[k]) {
              builders[k].appendNull();
            } else if (keyInfos.get(k).isVarchar) {
              SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
              if (dv != null) {
                BytesRef bytes = dv.lookupOrd(sgk.values[k]);
                VarcharType.VARCHAR.writeSlice(
                    builders[k], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              } else {
                VarcharType.VARCHAR.writeSlice(builders[k], Slices.EMPTY_SLICE);
              }
            } else {
              writeNumericKeyValue(builders[k], keyInfos.get(k), sgk.values[k]);
            }
          }

          for (int a = 0; a < numAggs; a++) {
            accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
          }
        }

        Block[] blocks = new Block[totalColumns];
        for (int i = 0; i < totalColumns; i++) {
          blocks[i] = builders[i].build();
        }
        return List.of(new Page(blocks));
      }

      // === Multi-segment path ===
      // Uses BytesRefKey (raw UTF-8 bytes) for cross-segment merge to avoid String allocation
      Map<MergedGroupKey, AccumulatorGroup> globalGroups = new LinkedHashMap<>();

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              Object[] keyReaders = new Object[keyInfos.size()];
              for (int i = 0; i < keyInfos.size(); i++) {
                KeyInfo ki = keyInfos.get(i);
                if (ki.isVarchar) {
                  keyReaders[i] = context.reader().getSortedSetDocValues(ki.name);
                } else {
                  keyReaders[i] = context.reader().getSortedNumericDocValues(ki.name);
                }
              }

              Object[] aggReaders = new Object[specs.size()];
              for (int i = 0; i < specs.size(); i++) {
                AggSpec spec = specs.get(i);
                if ("*".equals(spec.arg)) {
                  aggReaders[i] = null;
                } else if (spec.argType instanceof VarcharType) {
                  aggReaders[i] = context.reader().getSortedSetDocValues(spec.arg);
                } else {
                  aggReaders[i] = context.reader().getSortedNumericDocValues(spec.arg);
                }
              }

              Map<SegmentGroupKey, AccumulatorGroup> segmentGroups = new HashMap<>();
              final int numKeys = keyInfos.size();
              final NumericProbeKey probeKey = new NumericProbeKey(numKeys);

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  probeKey.reset();
                  for (int k = 0; k < numKeys; k++) {
                    KeyInfo ki = keyInfos.get(k);
                    if (ki.isVarchar) {
                      SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
                      if (dv != null && dv.advanceExact(doc)) {
                        probeKey.set(k, dv.nextOrd());
                      } else {
                        probeKey.setNull(k);
                      }
                    } else {
                      SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[k];
                      if (dv != null && dv.advanceExact(doc)) {
                        long val = dv.nextValue();
                        if (truncUnits[k] != null) {
                          val = truncateMillis(val, truncUnits[k]);
                        }
                        probeKey.set(k, val);
                      } else {
                        probeKey.setNull(k);
                      }
                    }
                  }
                  probeKey.computeHash();

                  AccumulatorGroup accGroup = segmentGroups.get(probeKey);
                  if (accGroup == null) {
                    SegmentGroupKey immutableKey = probeKey.toImmutableKey();
                    accGroup = createAccumulatorGroup(specs);
                    segmentGroups.put(immutableKey, accGroup);
                  }
                  accumulateDoc(doc, accGroup, specs, aggReaders);
                }

                @Override
                public void finish() throws IOException {
                  for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry :
                      segmentGroups.entrySet()) {
                    SegmentGroupKey sgk = entry.getKey();
                    AccumulatorGroup segAccs = entry.getValue();

                    Object[] resolvedKeys = new Object[keyInfos.size()];
                    for (int k = 0; k < keyInfos.size(); k++) {
                      if (sgk.nulls[k]) {
                        resolvedKeys[k] = null;
                      } else if (keyInfos.get(k).isVarchar) {
                        SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
                        if (dv != null) {
                          BytesRef bytes = dv.lookupOrd(sgk.values[k]);
                          resolvedKeys[k] = new BytesRefKey(bytes);
                        } else {
                          resolvedKeys[k] = new BytesRefKey(new BytesRef(""));
                        }
                      } else {
                        resolvedKeys[k] = sgk.values[k];
                      }
                    }

                    MergedGroupKey mgk = new MergedGroupKey(resolvedKeys, keyInfos);
                    AccumulatorGroup existing = globalGroups.get(mgk);
                    if (existing == null) {
                      globalGroups.put(mgk, segAccs);
                    } else {
                      existing.merge(segAccs);
                    }
                  }
                }
              };
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });

      if (globalGroups.isEmpty()) {
        return List.of();
      }

      int numGroupKeys = groupByKeys.size();
      int numAggs = specs.size();
      int totalColumns = numGroupKeys + numAggs;
      int groupCount = globalGroups.size();

      BlockBuilder[] builders = new BlockBuilder[totalColumns];
      for (int i = 0; i < numGroupKeys; i++) {
        builders[i] = keyInfos.get(i).type.createBlockBuilder(null, groupCount);
      }
      for (int i = 0; i < numAggs; i++) {
        builders[numGroupKeys + i] =
            resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
      }

      for (Map.Entry<MergedGroupKey, AccumulatorGroup> entry : globalGroups.entrySet()) {
        MergedGroupKey key = entry.getKey();
        AccumulatorGroup accGroup = entry.getValue();

        for (int k = 0; k < numGroupKeys; k++) {
          writeKeyValueForMerged(builders[k], keyInfos.get(k), key.values[k]);
        }

        for (int a = 0; a < numAggs; a++) {
          accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
        }
      }

      Block[] blocks = new Block[totalColumns];
      for (int i = 0; i < totalColumns; i++) {
        blocks[i] = builders[i].build();
      }
      return List.of(new Page(blocks));
    }
  }

  /** Resolve output types for the fused GROUP BY aggregate. */
  public static List<Type> resolveOutputTypes(
      AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    List<Type> types = new ArrayList<>();

    // Group-by key types
    for (String key : aggNode.getGroupByKeys()) {
      Type type = columnTypeMap.get(key);
      if (type != null) {
        types.add(type);
      } else {
        // Check for DATE_TRUNC expression — output type is timestamp
        Matcher dtm = DATE_TRUNC_PATTERN.matcher(key);
        if (dtm.matches()) {
          types.add(TimestampType.TIMESTAMP_MILLIS);
        } else {
          types.add(BigintType.BIGINT);
        }
      }
    }

    // Aggregate output types
    for (String func : aggNode.getAggregateFunctions()) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        types.add(BigintType.BIGINT);
        continue;
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      String arg = m.group(3).trim();
      switch (funcName) {
        case "COUNT":
          types.add(BigintType.BIGINT);
          break;
        case "AVG":
          types.add(DoubleType.DOUBLE);
          break;
        case "SUM":
          Type inputType = columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
          types.add(inputType instanceof DoubleType ? DoubleType.DOUBLE : BigintType.BIGINT);
          break;
        case "MIN":
        case "MAX":
          types.add(columnTypeMap.getOrDefault(arg, BigintType.BIGINT));
          break;
        default:
          types.add(BigintType.BIGINT);
      }
    }
    return types;
  }

  // =========================================================================
  // Internal structures
  // =========================================================================

  private record AggSpec(String funcName, boolean isDistinct, String arg, Type argType) {}

  /**
   * Metadata for a single GROUP BY key.
   *
   * @param name the column name to read from DocValues (for DATE_TRUNC, this is the source column)
   * @param type the output type of this key
   * @param isVarchar whether this key uses SortedSetDocValues (VARCHAR)
   * @param exprFunc the expression function name, or null for plain column references. Currently
   *     supports "date_trunc".
   * @param exprUnit the unit parameter for expression functions (e.g., "minute" for date_trunc), or
   *     null for plain column references.
   */
  private record KeyInfo(
      String name, Type type, boolean isVarchar, String exprFunc, String exprUnit) {}

  /**
   * Segment-local group key using ordinals for VARCHAR and raw values for numerics. Uses long
   * arrays for minimal allocation and fast hashing. Also serves as the base class for {@link
   * NumericProbeKey} to share the hashCode/equals contract.
   */
  private static class SegmentGroupKey {
    final long[] values;
    final boolean[] nulls;
    int hash;

    /** Standard constructor: copies arrays to ensure immutability. */
    SegmentGroupKey(long[] values, boolean[] nulls) {
      this.values = values.clone();
      this.nulls = nulls.clone();
      int h = 1;
      for (int i = 0; i < values.length; i++) {
        if (nulls[i]) {
          h = h * 31;
        } else {
          h = h * 31 + Long.hashCode(values[i]);
        }
      }
      this.hash = h;
    }

    /** Pre-allocation constructor for mutable probe keys (no cloning). */
    SegmentGroupKey(int size) {
      this.values = new long[size];
      this.nulls = new boolean[size];
      this.hash = 0;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof SegmentGroupKey other)) return false;
      if (this.hash != other.hash) return false;
      for (int i = 0; i < values.length; i++) {
        if (nulls[i] != other.nulls[i]) return false;
        if (!nulls[i] && values[i] != other.values[i]) return false;
      }
      return true;
    }
  }

  /**
   * Mutable probe key for HashMap lookups in the numeric-only GROUP BY path. Reused across
   * documents to avoid per-doc array allocation. The key is populated via {@link #set}/{@link
   * #setNull}, then {@link #computeHash()} is called before using it for HashMap.get().
   *
   * <p>This class intentionally shares the same hashCode/equals contract as {@link SegmentGroupKey}
   * so it can be used to probe a {@code HashMap<SegmentGroupKey, ...>} without creating an
   * immutable key for every document. An immutable copy is only created via {@link
   * #toImmutableKey()} when a new group needs to be inserted.
   */
  private static final class NumericProbeKey extends SegmentGroupKey {

    NumericProbeKey(int size) {
      // Initialize with pre-allocated arrays (no cloning needed for mutable probe)
      super(size);
    }

    /** Reset all nulls flags to false for reuse. Values are overwritten by set(). */
    void reset() {
      // Only need to clear nulls; values will be overwritten
      for (int i = 0; i < nulls.length; i++) {
        nulls[i] = false;
      }
    }

    void set(int index, long value) {
      values[index] = value;
    }

    void setNull(int index) {
      nulls[index] = true;
    }

    void computeHash() {
      int h = 1;
      for (int i = 0; i < values.length; i++) {
        if (nulls[i]) {
          h = h * 31;
        } else {
          h = h * 31 + Long.hashCode(values[i]);
        }
      }
      this.hash = h;
    }

    /** Create an immutable copy for insertion into the HashMap. */
    SegmentGroupKey toImmutableKey() {
      return new SegmentGroupKey(values, nulls);
    }
  }

  /**
   * Cross-segment merged group key using resolved values. VARCHAR keys are stored as BytesRefKey
   * (raw UTF-8 bytes, avoiding String allocation), numeric keys as Long or Double.
   */
  private static final class MergedGroupKey {
    final Object[] values;
    private final int hash;

    MergedGroupKey(Object[] values, List<KeyInfo> keyInfos) {
      this.values = values;
      int h = 1;
      for (Object v : values) {
        h = 31 * h + (v == null ? 0 : v.hashCode());
      }
      this.hash = h;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof MergedGroupKey other)) return false;
      if (this.hash != other.hash || this.values.length != other.values.length) return false;
      for (int i = 0; i < values.length; i++) {
        if (values[i] == null) {
          if (other.values[i] != null) return false;
        } else if (!values[i].equals(other.values[i])) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Immutable byte-array based key for use in HashMaps, avoiding String allocation. Created from
   * Lucene's BytesRef (which is a reference into a shared buffer), this class copies the bytes and
   * pre-computes hashCode. Used as a replacement for String keys in varchar GROUP BY to eliminate
   * the UTF-8-to-Java-char[] round-trip during aggregation.
   */
  private static final class BytesRefKey {
    final byte[] bytes;
    private final int hash;

    BytesRefKey(BytesRef ref) {
      // Copy bytes since BytesRef points into shared Lucene buffer
      this.bytes = new byte[ref.length];
      System.arraycopy(ref.bytes, ref.offset, this.bytes, 0, ref.length);
      this.hash = java.util.Arrays.hashCode(this.bytes);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof BytesRefKey other)) return false;
      return this.hash == other.hash && java.util.Arrays.equals(this.bytes, other.bytes);
    }
  }

  // =========================================================================
  // Accumulator infrastructure
  // =========================================================================

  /** Mergeable accumulator for cross-segment aggregation. */
  private interface MergeableAccumulator {
    void merge(MergeableAccumulator other);

    void writeTo(BlockBuilder builder);
  }

  /** Group of accumulators for a single group key. */
  private static final class AccumulatorGroup {
    final MergeableAccumulator[] accumulators;

    AccumulatorGroup(MergeableAccumulator[] accumulators) {
      this.accumulators = accumulators;
    }

    void merge(AccumulatorGroup other) {
      for (int i = 0; i < accumulators.length; i++) {
        accumulators[i].merge(other.accumulators[i]);
      }
    }
  }

  private static AccumulatorGroup createAccumulatorGroup(List<AggSpec> specs) {
    MergeableAccumulator[] accs = new MergeableAccumulator[specs.size()];
    for (int i = 0; i < specs.size(); i++) {
      accs[i] = createAccumulator(specs.get(i));
    }
    return new AccumulatorGroup(accs);
  }

  private static MergeableAccumulator createAccumulator(AggSpec spec) {
    switch (spec.funcName) {
      case "COUNT":
        if (spec.isDistinct) {
          return new CountDistinctAccum(spec.argType);
        }
        if ("*".equals(spec.arg)) {
          return new CountStarAccum();
        }
        return new CountStarAccum();
      case "SUM":
        return new SumAccum(spec.argType);
      case "MIN":
        return new MinAccum(spec.argType);
      case "MAX":
        return new MaxAccum(spec.argType);
      case "AVG":
        return new AvgAccum(spec.argType);
      default:
        throw new UnsupportedOperationException("Unsupported aggregate: " + spec.funcName);
    }
  }

  /** Feed one doc's values into the accumulator group. */
  private static void accumulateDoc(
      int doc, AccumulatorGroup accGroup, List<AggSpec> specs, Object[] aggReaders)
      throws IOException {
    for (int i = 0; i < specs.size(); i++) {
      AggSpec spec = specs.get(i);
      MergeableAccumulator acc = accGroup.accumulators[i];

      if ("*".equals(spec.arg)) {
        ((CountStarAccum) acc).count++;
        continue;
      }

      if (spec.argType instanceof VarcharType) {
        SortedSetDocValues dv = (SortedSetDocValues) aggReaders[i];
        if (dv != null && dv.advanceExact(doc)) {
          BytesRef bytes = dv.lookupOrd(dv.nextOrd());
          String val = bytes.utf8ToString();
          if (acc instanceof CountDistinctAccum cda) {
            cda.objectDistinctValues.add(val);
          } else if (acc instanceof MinAccum ma) {
            if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
              ma.objectVal = val;
              ma.hasValue = true;
            }
          } else if (acc instanceof MaxAccum xa) {
            if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
              xa.objectVal = val;
              xa.hasValue = true;
            }
          }
        }
      } else {
        SortedNumericDocValues dv = (SortedNumericDocValues) aggReaders[i];
        if (dv != null && dv.advanceExact(doc)) {
          long rawVal = dv.nextValue();
          boolean isDouble = spec.argType instanceof DoubleType;
          boolean isTimestamp = spec.argType instanceof TimestampType;

          if (spec.isDistinct && acc instanceof CountDistinctAccum cda) {
            if (cda.usePrimitiveLong) {
              cda.longDistinctValues.add(rawVal);
            } else {
              cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
            }
          } else if (acc instanceof CountStarAccum csa) {
            csa.count++;
          } else if (acc instanceof SumAccum sa) {
            sa.hasValue = true;
            if (isDouble) {
              sa.doubleSum += Double.longBitsToDouble(rawVal);
            } else {
              sa.longSum += rawVal;
            }
          } else if (acc instanceof MinAccum ma) {
            ma.hasValue = true;
            if (isDouble) {
              double d = Double.longBitsToDouble(rawVal);
              if (d < ma.doubleVal) ma.doubleVal = d;
            } else {
              if (rawVal < ma.longVal) ma.longVal = rawVal;
            }
          } else if (acc instanceof MaxAccum xa) {
            xa.hasValue = true;
            if (isDouble) {
              double d = Double.longBitsToDouble(rawVal);
              if (d > xa.doubleVal) xa.doubleVal = d;
            } else {
              if (rawVal > xa.longVal) xa.longVal = rawVal;
            }
          } else if (acc instanceof AvgAccum aa) {
            aa.count++;
            if (isDouble) {
              aa.doubleSum += Double.longBitsToDouble(rawVal);
            } else {
              aa.longSum += rawVal;
            }
          }
        }
      }
    }
  }

  // =========================================================================
  // Accumulator implementations (mergeable)
  // =========================================================================

  private static class CountStarAccum implements MergeableAccumulator {
    long count = 0;

    @Override
    public void merge(MergeableAccumulator other) {
      count += ((CountStarAccum) other).count;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      BigintType.BIGINT.writeLong(builder, count);
    }
  }

  private static class SumAccum implements MergeableAccumulator {
    final boolean isDouble;
    long longSum = 0;
    double doubleSum = 0;
    boolean hasValue = false;

    SumAccum(Type argType) {
      this.isDouble = argType instanceof DoubleType;
    }

    @Override
    public void merge(MergeableAccumulator other) {
      SumAccum o = (SumAccum) other;
      if (o.hasValue) {
        hasValue = true;
        longSum += o.longSum;
        doubleSum += o.doubleSum;
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isDouble) {
        DoubleType.DOUBLE.writeDouble(builder, doubleSum);
      } else {
        BigintType.BIGINT.writeLong(builder, longSum);
      }
    }
  }

  private static class MinAccum implements MergeableAccumulator {
    final Type argType;
    final boolean isDouble;
    final boolean isVarchar;
    final boolean isTimestamp;
    long longVal = Long.MAX_VALUE;
    double doubleVal = Double.MAX_VALUE;
    Object objectVal = null;
    boolean hasValue = false;

    MinAccum(Type argType) {
      this.argType = argType;
      this.isDouble = argType instanceof DoubleType;
      this.isVarchar = argType instanceof VarcharType;
      this.isTimestamp = argType instanceof TimestampType;
    }

    @Override
    public void merge(MergeableAccumulator other) {
      MinAccum o = (MinAccum) other;
      if (!o.hasValue) return;
      hasValue = true;
      if (isVarchar) {
        if (objectVal == null || ((String) o.objectVal).compareTo((String) objectVal) < 0) {
          objectVal = o.objectVal;
        }
      } else if (isDouble) {
        if (o.doubleVal < doubleVal) doubleVal = o.doubleVal;
      } else {
        if (o.longVal < longVal) longVal = o.longVal;
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isVarchar) {
        VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice((String) objectVal));
      } else if (isDouble) {
        DoubleType.DOUBLE.writeDouble(builder, doubleVal);
      } else if (isTimestamp) {
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, longVal * 1000L);
      } else {
        argType.writeLong(builder, longVal);
      }
    }
  }

  private static class MaxAccum implements MergeableAccumulator {
    final Type argType;
    final boolean isDouble;
    final boolean isVarchar;
    final boolean isTimestamp;
    long longVal = Long.MIN_VALUE;
    double doubleVal = -Double.MAX_VALUE;
    Object objectVal = null;
    boolean hasValue = false;

    MaxAccum(Type argType) {
      this.argType = argType;
      this.isDouble = argType instanceof DoubleType;
      this.isVarchar = argType instanceof VarcharType;
      this.isTimestamp = argType instanceof TimestampType;
    }

    @Override
    public void merge(MergeableAccumulator other) {
      MaxAccum o = (MaxAccum) other;
      if (!o.hasValue) return;
      hasValue = true;
      if (isVarchar) {
        if (objectVal == null || ((String) o.objectVal).compareTo((String) objectVal) > 0) {
          objectVal = o.objectVal;
        }
      } else if (isDouble) {
        if (o.doubleVal > doubleVal) doubleVal = o.doubleVal;
      } else {
        if (o.longVal > longVal) longVal = o.longVal;
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isVarchar) {
        VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice((String) objectVal));
      } else if (isDouble) {
        DoubleType.DOUBLE.writeDouble(builder, doubleVal);
      } else if (isTimestamp) {
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, longVal * 1000L);
      } else {
        argType.writeLong(builder, longVal);
      }
    }
  }

  private static class AvgAccum implements MergeableAccumulator {
    final boolean isDouble;
    long longSum = 0;
    double doubleSum = 0;
    long count = 0;

    AvgAccum(Type argType) {
      this.isDouble = argType instanceof DoubleType;
    }

    @Override
    public void merge(MergeableAccumulator other) {
      AvgAccum o = (AvgAccum) other;
      longSum += o.longSum;
      doubleSum += o.doubleSum;
      count += o.count;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (count == 0) {
        builder.appendNull();
      } else if (isDouble) {
        DoubleType.DOUBLE.writeDouble(builder, doubleSum / count);
      } else {
        DoubleType.DOUBLE.writeDouble(builder, (double) longSum / count);
      }
    }
  }

  /**
   * COUNT(DISTINCT) accumulator for grouped aggregation. Uses primitive LongOpenHashSet for numeric
   * non-double columns to avoid Long boxing overhead. Falls back to HashSet&lt;Object&gt; for
   * varchar and double types.
   */
  private static class CountDistinctAccum implements MergeableAccumulator {
    final Type argType;
    final boolean usePrimitiveLong;

    /** Primitive long set for numeric non-double columns (avoids Long boxing). */
    final LongOpenHashSet longDistinctValues;

    /** Fallback set for varchar and double types. */
    final Set<Object> objectDistinctValues;

    CountDistinctAccum(Type argType) {
      this.argType = argType;
      this.usePrimitiveLong = !(argType instanceof VarcharType) && !(argType instanceof DoubleType);
      this.longDistinctValues = usePrimitiveLong ? new LongOpenHashSet() : null;
      this.objectDistinctValues = usePrimitiveLong ? null : new HashSet<>();
    }

    @Override
    public void merge(MergeableAccumulator other) {
      CountDistinctAccum o = (CountDistinctAccum) other;
      if (usePrimitiveLong) {
        longDistinctValues.addAll(o.longDistinctValues);
      } else {
        objectDistinctValues.addAll(o.objectDistinctValues);
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      long count = usePrimitiveLong ? longDistinctValues.size() : objectDistinctValues.size();
      BigintType.BIGINT.writeLong(builder, count);
    }
  }

  // =========================================================================
  // Output helpers
  // =========================================================================

  private static Type resolveAggOutputType(AggSpec spec, Map<String, Type> columnTypeMap) {
    switch (spec.funcName) {
      case "COUNT":
        return BigintType.BIGINT;
      case "AVG":
        return DoubleType.DOUBLE;
      case "SUM":
        Type inputType = columnTypeMap.getOrDefault(spec.arg, BigintType.BIGINT);
        return inputType instanceof DoubleType ? DoubleType.DOUBLE : BigintType.BIGINT;
      case "MIN":
      case "MAX":
        return columnTypeMap.getOrDefault(spec.arg, BigintType.BIGINT);
      default:
        return BigintType.BIGINT;
    }
  }

  /**
   * Write a group-by key value to a block builder for the multi-segment merged path. VARCHAR keys
   * are stored as BytesRefKey (raw UTF-8 bytes), numeric keys as Long.
   */
  private static void writeKeyValueForMerged(BlockBuilder builder, KeyInfo keyInfo, Object value) {
    if (value == null) {
      builder.appendNull();
      return;
    }
    Type type = keyInfo.type;
    if (type instanceof VarcharType) {
      if (value instanceof BytesRefKey brk) {
        VarcharType.VARCHAR.writeSlice(builder, Slices.wrappedBuffer(brk.bytes));
      } else {
        VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice((String) value));
      }
    } else if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, Double.longBitsToDouble((Long) value));
    } else if (type instanceof TimestampType) {
      TimestampType.TIMESTAMP_MILLIS.writeLong(builder, ((Long) value) * 1000L);
    } else if (type instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, ((Long) value) == 1);
    } else {
      // BigintType, IntegerType, SmallintType, TinyintType
      type.writeLong(builder, (Long) value);
    }
  }

  /** Write a numeric group-by key value (raw long) to a block builder. */
  private static void writeNumericKeyValue(BlockBuilder builder, KeyInfo keyInfo, long value) {
    Type type = keyInfo.type;
    if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, Double.longBitsToDouble(value));
    } else if (type instanceof TimestampType) {
      TimestampType.TIMESTAMP_MILLIS.writeLong(builder, value * 1000L);
    } else if (type instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, value == 1);
    } else {
      // BigintType, IntegerType, SmallintType, TinyintType
      type.writeLong(builder, value);
    }
  }
}
