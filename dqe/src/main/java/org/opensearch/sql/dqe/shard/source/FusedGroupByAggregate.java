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
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
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

  private FusedGroupByAggregate() {}

  /**
   * Check if the shard plan is a GROUP BY aggregation that can use the fused path. Requirements:
   *
   * <ul>
   *   <li>Plan is AggregationNode with non-empty groupByKeys
   *   <li>Child is a TableScanNode (no intermediate filter/eval nodes)
   *   <li>All group-by keys are VARCHAR, numeric, or timestamp types
   *   <li>All aggregate functions are supported
   * </ul>
   */
  public static boolean canFuse(AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    if (aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    if (!(aggNode.getChild() instanceof TableScanNode)) {
      return false;
    }

    // Check all group-by keys are supported types (VARCHAR, numeric, or timestamp)
    for (String key : aggNode.getGroupByKeys()) {
      Type type = columnTypeMap.get(key);
      if (type == null) {
        return false; // Unknown type, can't fuse
      } else if (!(type instanceof VarcharType) && !isNumericOrTimestamp(type)) {
        return false; // Unsupported group-by key type
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
    TableScanNode scanNode = (TableScanNode) aggNode.getChild();
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

    // Classify group-by keys
    List<KeyInfo> keyInfos = new ArrayList<>();
    boolean hasVarchar = false;
    for (String key : groupByKeys) {
      Type type = columnTypeMap.get(key);
      boolean isVarchar = type instanceof VarcharType;
      keyInfos.add(new KeyInfo(key, type, isVarchar));
      if (isVarchar) {
        hasVarchar = true;
      }
    }

    // Dispatch to specialized path based on key types
    if (hasVarchar) {
      return executeWithVarcharKeys(shard, query, keyInfos, specs, columnTypeMap, groupByKeys);
    } else {
      return executeNumericOnly(shard, query, keyInfos, specs, columnTypeMap, groupByKeys);
    }
  }

  /**
   * Fast path for numeric-only GROUP BY keys. Since all keys are raw long values (no segment-local
   * ordinals), we aggregate directly into a global map across all segments without
   * per-segment/cross-segment merge overhead.
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

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric")) {

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              // Open doc values for group-by keys (all numeric)
              SortedNumericDocValues[] keyReaders = new SortedNumericDocValues[keyInfos.size()];
              for (int i = 0; i < keyInfos.size(); i++) {
                keyReaders[i] = context.reader().getSortedNumericDocValues(keyInfos.get(i).name);
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

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  long[] keyValues = new long[keyInfos.size()];
                  boolean[] keyNulls = new boolean[keyInfos.size()];

                  for (int k = 0; k < keyInfos.size(); k++) {
                    SortedNumericDocValues dv = keyReaders[k];
                    if (dv != null && dv.advanceExact(doc)) {
                      keyValues[k] = dv.nextValue();
                    } else {
                      keyNulls[k] = true;
                    }
                  }

                  SegmentGroupKey sgk = new SegmentGroupKey(keyValues, keyNulls);
                  AccumulatorGroup accGroup =
                      globalGroups.computeIfAbsent(sgk, ignored -> createAccumulatorGroup(specs));
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
   * ordinals as hash keys during per-segment aggregation, then resolves ordinals to strings across
   * segments.
   */
  private static List<Page> executeWithVarcharKeys(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys)
      throws Exception {

    Map<MergedGroupKey, AccumulatorGroup> globalGroups = new LinkedHashMap<>();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby")) {

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              // Open doc values for group-by keys
              Object[] keyReaders = new Object[keyInfos.size()];
              for (int i = 0; i < keyInfos.size(); i++) {
                KeyInfo ki = keyInfos.get(i);
                if (ki.isVarchar) {
                  keyReaders[i] = context.reader().getSortedSetDocValues(ki.name);
                } else {
                  keyReaders[i] = context.reader().getSortedNumericDocValues(ki.name);
                }
              }

              // Open doc values for aggregate arguments
              Object[] aggReaders = new Object[specs.size()];
              for (int i = 0; i < specs.size(); i++) {
                AggSpec spec = specs.get(i);
                if ("*".equals(spec.arg)) {
                  aggReaders[i] = null; // COUNT(*)
                } else if (spec.argType instanceof VarcharType) {
                  aggReaders[i] = context.reader().getSortedSetDocValues(spec.arg);
                } else {
                  aggReaders[i] = context.reader().getSortedNumericDocValues(spec.arg);
                }
              }

              Map<SegmentGroupKey, AccumulatorGroup> segmentGroups = new HashMap<>();

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  long[] keyValues = new long[keyInfos.size()];
                  boolean[] keyNulls = new boolean[keyInfos.size()];

                  for (int k = 0; k < keyInfos.size(); k++) {
                    KeyInfo ki = keyInfos.get(k);
                    if (ki.isVarchar) {
                      SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
                      if (dv != null && dv.advanceExact(doc)) {
                        keyValues[k] = dv.nextOrd();
                      } else {
                        keyNulls[k] = true;
                      }
                    } else {
                      SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[k];
                      if (dv != null && dv.advanceExact(doc)) {
                        keyValues[k] = dv.nextValue();
                      } else {
                        keyNulls[k] = true;
                      }
                    }
                  }

                  SegmentGroupKey sgk = new SegmentGroupKey(keyValues, keyNulls);
                  AccumulatorGroup accGroup =
                      segmentGroups.computeIfAbsent(sgk, ignored -> createAccumulatorGroup(specs));
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
                          resolvedKeys[k] = bytes.utf8ToString();
                        } else {
                          resolvedKeys[k] = "";
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
    }

    if (globalGroups.isEmpty()) {
      return List.of();
    }

    // Build result Page
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
        writeKeyValue(builders[k], keyInfos.get(k), key.values[k]);
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

  /** Resolve output types for the fused GROUP BY aggregate. */
  public static List<Type> resolveOutputTypes(
      AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    List<Type> types = new ArrayList<>();

    // Group-by key types
    for (String key : aggNode.getGroupByKeys()) {
      types.add(columnTypeMap.getOrDefault(key, BigintType.BIGINT));
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

  private record KeyInfo(String name, Type type, boolean isVarchar) {}

  /**
   * Segment-local group key using ordinals for VARCHAR and raw values for numerics. Uses long
   * arrays for minimal allocation and fast hashing.
   */
  private static final class SegmentGroupKey {
    final long[] values;
    final boolean[] nulls;
    private final int hash;

    SegmentGroupKey(long[] values, boolean[] nulls) {
      // Copy to ensure immutability
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
   * Cross-segment merged group key using resolved string values. VARCHAR keys are stored as String,
   * numeric keys as Long or Double.
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
            cda.distinctValues.add(val);
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
            cda.distinctValues.add(isDouble ? Double.longBitsToDouble(rawVal) : rawVal);
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

  private static class CountDistinctAccum implements MergeableAccumulator {
    final Type argType;
    final Set<Object> distinctValues = new HashSet<>();

    CountDistinctAccum(Type argType) {
      this.argType = argType;
    }

    @Override
    public void merge(MergeableAccumulator other) {
      distinctValues.addAll(((CountDistinctAccum) other).distinctValues);
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      BigintType.BIGINT.writeLong(builder, distinctValues.size());
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

  /** Write a group-by key value to a block builder. */
  private static void writeKeyValue(BlockBuilder builder, KeyInfo keyInfo, Object value) {
    if (value == null) {
      builder.appendNull();
      return;
    }
    Type type = keyInfo.type;
    if (type instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice((String) value));
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
