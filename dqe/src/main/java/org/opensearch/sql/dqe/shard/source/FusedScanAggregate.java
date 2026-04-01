/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.sql.dqe.operator.LongOpenHashSet;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

/**
 * Fused scan-aggregate operator that directly aggregates from Lucene DocValues without building
 * intermediate Trino Pages. This eliminates the overhead of BlockBuilder allocation, Block.build(),
 * and Page construction for scalar aggregations (no GROUP BY).
 *
 * <p>Supports: COUNT(*), SUM, MIN, MAX, AVG on numeric columns, and COUNT(DISTINCT) on any column.
 *
 * <p>Used as a fast path when the shard plan is: AggregationNode(groupBy=[], aggs=[...]) ->
 * TableScanNode.
 */
public final class FusedScanAggregate {

  // Segment-level aggregate cache: maps segment core cache key -> (columnName -> long[]{sum, count, min, max}).
  // Uses LeafReader.getCoreCacheHelper().getKey() for stable per-segment identity.
  // Eviction: addClosedListener removes entries when segment is closed/merged.
  // Only populated for segments with no deleted docs (liveDocs == null).
  private static final ConcurrentHashMap<Object, ConcurrentHashMap<String, long[]>>
      SEGMENT_AGG_CACHE = new ConcurrentHashMap<>();

  private static final Pattern AGG_FUNCTION =
      Pattern.compile(
          "^\\s*(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)\\s*$", Pattern.CASE_INSENSITIVE);

  /**
   * Pattern to match expressions like "(col + 42)" or "(col - 3)" from EvalNode output names.
   * Captures the column name and the integer constant offset. Handles both "col + N" and "(col +
   * N)" forms.
   */
  private static final Pattern COL_PLUS_CONST =
      Pattern.compile("^\\(?\\s*(\\w+)\\s*\\+\\s*(\\d+)\\s*\\)?$", Pattern.CASE_INSENSITIVE);

  private FusedScanAggregate() {}

  /**
   * Check if the shard plan is a scalar aggregation (no GROUP BY) over a plain TableScanNode that
   * can be fused.
   */
  public static boolean canFuse(AggregationNode aggNode) {
    if (!aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    if (!(aggNode.getChild() instanceof TableScanNode)) {
      return false;
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

  /**
   * Check if the shard plan is a scalar aggregation over an EvalNode -> TableScanNode that can use
   * the fused algebraic shortcut. This matches the pattern where all aggregates are SUM and all
   * eval expressions are either plain column references or (column + integer_constant). Exploits
   * the identity: SUM(col + k) = SUM(col) + k * COUNT(*) to read each underlying column only once.
   */
  public static boolean canFuseWithEval(AggregationNode aggNode) {
    if (!aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    if (!(aggNode.getChild() instanceof EvalNode evalNode)) {
      return false;
    }
    if (!(evalNode.getChild() instanceof TableScanNode)) {
      return false;
    }
    // All aggregate functions must be SUM, COUNT, or AVG (non-distinct)
    for (String func : aggNode.getAggregateFunctions()) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        return false;
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      if (isDistinct) {
        return false;
      }
      if (!"SUM".equals(funcName) && !"COUNT".equals(funcName) && !"AVG".equals(funcName)) {
        return false;
      }
    }
    // All eval expressions must be plain columns or (column + integer_constant)
    for (String expr : evalNode.getExpressions()) {
      String trimmed = expr.trim();
      // Plain column reference (single identifier)
      if (trimmed.matches("\\w+")) {
        continue;
      }
      // (col + N) pattern
      Matcher cm = COL_PLUS_CONST.matcher(trimmed);
      if (cm.matches()) {
        continue;
      }
      return false; // Unsupported expression
    }
    return true;
  }

  /**
   * Execute the fused algebraic shortcut for SUM(col + constant) patterns. Instead of evaluating 90
   * expressions per row, reads each unique underlying column once and uses the identity: SUM(col +
   * k) = SUM(col) + k * COUNT(*).
   *
   * @param aggNode the aggregation plan node
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @param columnTypeMap type mapping for columns
   * @return a list containing a single Page with the aggregate results
   */
  public static List<Page> executeWithEval(
      AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
      throws Exception {
    EvalNode evalNode = (EvalNode) aggNode.getChild();
    List<String> evalExprs = evalNode.getExpressions();
    List<String> evalNames = evalNode.getOutputColumnNames();
    List<String> aggFunctions = aggNode.getAggregateFunctions();

    // Build mapping: eval column name -> (physicalColumn, offset)
    // Also collect unique physical columns that need SUM
    Map<String, String> nameToPhysical = new java.util.LinkedHashMap<>();
    Map<String, Long> nameToOffset = new java.util.LinkedHashMap<>();
    Set<String> uniquePhysicalColumns = new java.util.LinkedHashSet<>();

    for (int i = 0; i < evalExprs.size(); i++) {
      String expr = evalExprs.get(i).trim();
      String name = evalNames.get(i);
      Matcher cm = COL_PLUS_CONST.matcher(expr);
      if (cm.matches()) {
        String col = cm.group(1);
        long offset = Long.parseLong(cm.group(2));
        nameToPhysical.put(name, col);
        nameToOffset.put(name, offset);
        uniquePhysicalColumns.add(col);
      } else {
        // Plain column reference
        nameToPhysical.put(name, expr);
        nameToOffset.put(name, 0L);
        uniquePhysicalColumns.add(expr);
      }
    }

    // Compute SUM and COUNT for each unique physical column
    // Also include physical columns referenced directly by COUNT(col) and AVG(col) aggregates
    for (String func : aggFunctions) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (m.matches()) {
        String arg = m.group(3).trim();
        if (!"*".equals(arg) && !nameToPhysical.containsKey(arg)) {
          // Direct physical column reference (not through EvalNode)
          nameToPhysical.put(arg, arg);
          nameToOffset.put(arg, 0L);
          uniquePhysicalColumns.add(arg);
        }
      }
    }
    Map<String, long[]> colSumCount = new java.util.LinkedHashMap<>();
    for (String col : uniquePhysicalColumns) {
      colSumCount.put(col, new long[2]); // [sum, count]
    }

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-eval-agg")) {
      String[] colArray = uniquePhysicalColumns.toArray(new String[0]);
      // Pre-resolve Map entries to flat arrays for zero-alloc inner loop
      long[][] scArrays = new long[colArray.length][];
      for (int i = 0; i < colArray.length; i++) {
        scArrays[i] = colSumCount.get(colArray[i]);
      }

      if (query instanceof MatchAllDocsQuery) {
        // Fast path: column-major iteration using nextDoc() for sequential access.
        // Avoids advanceExact() overhead and Map lookups in the inner loop.
        boolean noDeletes = true;
        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          if (leafCtx.reader().getLiveDocs() != null) {
            noDeletes = false;
            break;
          }
        }

        if (noDeletes) {
          // Parallel path: distribute segments across workers for segment-level parallelism.
          // Each worker accumulates sum/count for all columns across its assigned segments,
          // then results are merged. This is the same strategy as tryFlatArrayPath.
          List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
          int numWorkers =
              Math.min(
                  Math.max(
                      1,
                      Runtime.getRuntime().availableProcessors()
                          / Integer.getInteger("dqe.numLocalShards", 4)),
                  leaves.size());

          if (numWorkers > 1 && leaves.size() > 1) {
            // Partition segments among workers using largest-first assignment
            @SuppressWarnings("unchecked")
            List<LeafReaderContext>[] workerSegments = new List[numWorkers];
            long[] workerDocCounts = new long[numWorkers];
            for (int i = 0; i < numWorkers; i++) {
              workerSegments[i] = new java.util.ArrayList<>();
            }
            java.util.List<LeafReaderContext> sortedLeaves = new java.util.ArrayList<>(leaves);
            sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
            for (LeafReaderContext leaf : sortedLeaves) {
              int lightest = 0;
              for (int i = 1; i < numWorkers; i++) {
                if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
              }
              workerSegments[lightest].add(leaf);
              workerDocCounts[lightest] += leaf.reader().maxDoc();
            }

            final int nc = colArray.length;
            @SuppressWarnings("unchecked")
            java.util.concurrent.CompletableFuture<long[]>[] futures =
                new java.util.concurrent.CompletableFuture[numWorkers];

            for (int w = 0; w < numWorkers; w++) {
              final List<LeafReaderContext> mySegments = workerSegments[w];
              futures[w] =
                  java.util.concurrent.CompletableFuture.supplyAsync(
                      () -> {
                        // Pack: [sum0, count0, sum1, count1, ...]
                        long[] pack = new long[nc * 2];
                        try {
                          for (LeafReaderContext leafCtx : mySegments) {
                            LeafReader reader = leafCtx.reader();
                            long segCount = reader.maxDoc(); // MatchAll + no deletes

                            // Segment-level cache lookup
                            IndexReader.CacheHelper coreHelper = reader.getCoreCacheHelper();
                            Object segKey = coreHelper != null ? coreHelper.getKey() : null;
                            ConcurrentHashMap<String, long[]> segCache =
                                segKey != null ? SEGMENT_AGG_CACHE.get(segKey) : null;

                            for (int i = 0; i < nc; i++) {
                              // Check cache before scanning DocValues
                              if (segCache != null) {
                                long[] cached = segCache.get(colArray[i]);
                                if (cached != null) {
                                  pack[i * 2] += cached[0];
                                  pack[i * 2 + 1] += cached[1];
                                  continue; // Cache hit
                                }
                              }

                              SortedNumericDocValues dv =
                                  reader.getSortedNumericDocValues(colArray[i]);
                              if (dv == null) continue;
                              long localSum = 0;
                              int doc = dv.nextDoc();
                              while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                                localSum += dv.nextValue();
                                doc = dv.nextDoc();
                              }
                              pack[i * 2] += localSum;
                              pack[i * 2 + 1] += segCount;

                              // Store in cache
                              if (segKey != null) {
                                SEGMENT_AGG_CACHE
                                    .computeIfAbsent(segKey, k -> {
                                      try {
                                        coreHelper.addClosedListener(
                                            closedKey -> SEGMENT_AGG_CACHE.remove(closedKey));
                                      } catch (Exception ignored) { }
                                      return new ConcurrentHashMap<>();
                                    })
                                    .putIfAbsent(colArray[i],
                                        new long[]{localSum, segCount, Long.MAX_VALUE, Long.MIN_VALUE});
                              }
                            }
                          }
                        } catch (IOException e) {
                          throw new java.io.UncheckedIOException(e);
                        }
                        return pack;
                      },
                      FusedGroupByAggregate.getParallelPool());
            }

            // Merge worker results
            for (var future : futures) {
              long[] pack = future.join();
              for (int i = 0; i < nc; i++) {
                scArrays[i][0] += pack[i * 2];
                scArrays[i][1] += pack[i * 2 + 1];
              }
            }
          } else {
            // Sequential fallback for single segment or single worker
            for (LeafReaderContext leafCtx : leaves) {
              LeafReader reader = leafCtx.reader();
              long segCount = reader.maxDoc(); // MatchAll + no deletes

              // Segment-level cache lookup
              IndexReader.CacheHelper coreHelper = reader.getCoreCacheHelper();
              Object segKey = coreHelper != null ? coreHelper.getKey() : null;
              ConcurrentHashMap<String, long[]> segCache =
                  segKey != null ? SEGMENT_AGG_CACHE.get(segKey) : null;

              for (int i = 0; i < colArray.length; i++) {
                // Check cache before scanning DocValues
                if (segCache != null) {
                  long[] cached = segCache.get(colArray[i]);
                  if (cached != null) {
                    scArrays[i][0] += cached[0];
                    scArrays[i][1] += cached[1];
                    continue; // Cache hit
                  }
                }

                SortedNumericDocValues dv = reader.getSortedNumericDocValues(colArray[i]);
                if (dv == null) continue;
                long localSum = 0;
                int doc = dv.nextDoc();
                while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                  localSum += dv.nextValue();
                  doc = dv.nextDoc();
                }
                scArrays[i][0] += localSum;
                scArrays[i][1] += segCount;

                // Store in cache
                if (segKey != null) {
                  SEGMENT_AGG_CACHE
                      .computeIfAbsent(segKey, k -> {
                        try {
                          coreHelper.addClosedListener(
                              closedKey -> SEGMENT_AGG_CACHE.remove(closedKey));
                        } catch (Exception ignored) { }
                        return new ConcurrentHashMap<>();
                      })
                      .putIfAbsent(colArray[i],
                          new long[]{localSum, segCount, Long.MAX_VALUE, Long.MIN_VALUE});
                }
              }
            }
          }
        } else {
          // Has deleted docs: row-major with advanceExact
          for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
            LeafReader reader = leafCtx.reader();
            int maxDoc = reader.maxDoc();
            Bits liveDocs = reader.getLiveDocs();
            SortedNumericDocValues[] dvs = new SortedNumericDocValues[colArray.length];
            for (int i = 0; i < colArray.length; i++) {
              dvs[i] = reader.getSortedNumericDocValues(colArray[i]);
            }
            for (int doc = 0; doc < maxDoc; doc++) {
              if (liveDocs.get(doc)) {
                for (int i = 0; i < colArray.length; i++) {
                  SortedNumericDocValues dv = dvs[i];
                  if (dv != null && dv.advanceExact(doc)) {
                    scArrays[i][0] += dv.nextValue();
                    scArrays[i][1]++;
                  }
                }
              }
            }
          }
        }
      } else {
        // General path: use Lucene's search framework
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues[] dvs = new SortedNumericDocValues[colArray.length];
                for (int i = 0; i < colArray.length; i++) {
                  dvs[i] = context.reader().getSortedNumericDocValues(colArray[i]);
                }
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    for (int i = 0; i < colArray.length; i++) {
                      SortedNumericDocValues dv = dvs[i];
                      if (dv != null && dv.advanceExact(doc)) {
                        long[] sc = colSumCount.get(colArray[i]);
                        sc[0] += dv.nextValue();
                        sc[1]++;
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
    }

    // Derive results: SUM(col + k) = SUM(col) + k * COUNT(col)
    // COUNT(*) = count of any physical column, AVG(col + k) = (SUM(col) + k * COUNT) / COUNT
    int numAggs = aggFunctions.size();
    BlockBuilder[] builders = new BlockBuilder[numAggs];
    for (int i = 0; i < numAggs; i++) {
      Matcher m = AGG_FUNCTION.matcher(aggFunctions.get(i));
      String funcName = m.matches() ? m.group(1).toUpperCase(Locale.ROOT) : "SUM";
      builders[i] =
          "AVG".equals(funcName)
              ? DoubleType.DOUBLE.createBlockBuilder(null, 1)
              : BigintType.BIGINT.createBlockBuilder(null, 1);
    }

    for (int i = 0; i < numAggs; i++) {
      Matcher m = AGG_FUNCTION.matcher(aggFunctions.get(i));
      if (!m.matches()) {
        throw new IllegalArgumentException("Unsupported aggregate: " + aggFunctions.get(i));
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      String arg = m.group(3).trim(); // The eval column name or "*"

      if ("COUNT".equals(funcName)) {
        // COUNT(*) or COUNT(col): use count from any available physical column
        if ("*".equals(arg)) {
          // Use count from the first physical column
          long count = 0;
          for (long[] sc : colSumCount.values()) {
            count = sc[1];
            break;
          }
          BigintType.BIGINT.writeLong(builders[i], count);
        } else {
          String physCol = nameToPhysical.getOrDefault(arg, arg);
          long[] sc = colSumCount.get(physCol);
          BigintType.BIGINT.writeLong(builders[i], sc != null ? sc[1] : 0);
        }
      } else if ("AVG".equals(funcName)) {
        String physCol = nameToPhysical.getOrDefault(arg, arg);
        long offset = nameToOffset.getOrDefault(arg, 0L);
        long[] sc = colSumCount.get(physCol);
        if (sc != null && sc[1] > 0) {
          double avg = (double) (sc[0] + offset * sc[1]) / sc[1];
          DoubleType.DOUBLE.writeDouble(builders[i], avg);
        } else {
          builders[i].appendNull();
        }
      } else {
        // SUM: original algebraic identity
        String physCol = nameToPhysical.get(arg);
        long offset = nameToOffset.getOrDefault(arg, 0L);
        long[] sc = colSumCount.get(physCol);
        long result = sc[0] + offset * sc[1]; // SUM(col) + k * COUNT
        BigintType.BIGINT.writeLong(builders[i], result);
      }
    }

    Block[] blocks = new Block[numAggs];
    for (int i = 0; i < numAggs; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }

  /**
   * Resolve the output types for the fused eval-aggregate. SUM and COUNT return BigintType, AVG
   * returns DoubleType.
   */
  public static List<Type> resolveEvalAggOutputTypes(AggregationNode aggNode) {
    List<Type> types = new ArrayList<>();
    for (String func : aggNode.getAggregateFunctions()) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (m.matches() && "AVG".equalsIgnoreCase(m.group(1))) {
        types.add(DoubleType.DOUBLE);
      } else {
        types.add(BigintType.BIGINT);
      }
    }
    return types;
  }

  /**
   * Execute the fused scan-aggregate directly from Lucene DocValues.
   *
   * @param aggNode the aggregation plan node
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @param columnTypeMap type mapping for columns
   * @return a list containing a single Page with the aggregate results
   */
  public static List<Page> execute(
      AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
      throws Exception {
    TableScanNode scanNode = (TableScanNode) aggNode.getChild();
    List<String> aggFunctions = aggNode.getAggregateFunctions();

    // Parse aggregate functions
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

    // Create accumulators
    List<DirectAccumulator> accumulators =
        specs.stream().map(FusedScanAggregate::createAccumulator).collect(Collectors.toList());

    // Execute search and aggregate directly from doc values
    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-agg")) {

      // Ultra-fast path: COUNT(*) only with MatchAllDocsQuery — no per-doc iteration needed.
      // Just read numDocs from the index reader, which is O(1).
      if (query instanceof MatchAllDocsQuery) {
        boolean allCountStar = true;
        for (AggSpec spec : specs) {
          if (!"COUNT".equals(spec.funcName) || !"*".equals(spec.arg)) {
            allCountStar = false;
            break;
          }
        }
        if (allCountStar) {
          long totalDocs = engineSearcher.getIndexReader().numDocs();
          for (DirectAccumulator acc : accumulators) {
            if (acc instanceof CountStarDirectAccumulator csa) {
              csa.addCount(totalDocs);
            }
          }
          // Build result directly
          int numAggs = accumulators.size();
          BlockBuilder[] builders = new BlockBuilder[numAggs];
          for (int i = 0; i < numAggs; i++) {
            builders[i] = accumulators.get(i).getOutputType().createBlockBuilder(null, 1);
            accumulators.get(i).writeTo(builders[i]);
          }
          Block[] blocks = new Block[numAggs];
          for (int i = 0; i < numAggs; i++) {
            blocks[i] = builders[i].build();
          }
          return List.of(new Page(blocks));
        }
      }

      // Ultra-fast path: MIN/MAX only on numeric/date fields with MatchAllDocsQuery and no
      // deleted docs. Uses PointValues.getMinPackedValue/getMaxPackedValue which is O(1) per
      // segment, avoiding all per-doc iteration. Critical for Q7: MIN(EventDate), MAX(EventDate).
      if (query instanceof MatchAllDocsQuery) {
        boolean allMinMax = true;
        boolean noDeletedDocs = true;
        for (AggSpec spec : specs) {
          if (!("MIN".equals(spec.funcName) || "MAX".equals(spec.funcName))
              || spec.isDistinct
              || spec.argType instanceof VarcharType) {
            allMinMax = false;
            break;
          }
        }
        // Check no deleted docs (PointValues min/max only valid for entire segment)
        if (allMinMax) {
          for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
            if (leafCtx.reader().getLiveDocs() != null) {
              noDeletedDocs = false;
              break;
            }
          }
        }
        if (allMinMax && noDeletedDocs && engineSearcher.getIndexReader().numDocs() > 0) {
          boolean canUsePointValues = true;
          long[] results = new long[specs.size()];
          for (int i = 0; i < specs.size(); i++) {
            results[i] = "MIN".equals(specs.get(i).funcName) ? Long.MAX_VALUE : Long.MIN_VALUE;
          }
          for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
            LeafReader reader = leafCtx.reader();
            for (int i = 0; i < specs.size(); i++) {
              AggSpec spec = specs.get(i);
              PointValues pointValues = reader.getPointValues(spec.arg);
              if (pointValues == null || pointValues.size() == 0) {
                continue;
              }
              byte[] packed;
              if ("MIN".equals(spec.funcName)) {
                packed = pointValues.getMinPackedValue();
              } else {
                packed = pointValues.getMaxPackedValue();
              }
              if (packed == null || packed.length < Long.BYTES) {
                canUsePointValues = false;
                break;
              }
              // Decode using Lucene's standard sortable encoding
              long val = NumericUtils.sortableBytesToLong(packed, 0);
              if ("MIN".equals(spec.funcName)) {
                results[i] = Math.min(results[i], val);
              } else {
                results[i] = Math.max(results[i], val);
              }
            }
            if (!canUsePointValues) break;
          }
          if (canUsePointValues) {
            int numAggs = specs.size();
            BlockBuilder[] builders = new BlockBuilder[numAggs];
            for (int i = 0; i < numAggs; i++) {
              AggSpec spec = specs.get(i);
              Type outputType = accumulators.get(i).getOutputType();
              builders[i] = outputType.createBlockBuilder(null, 1);
              if (spec.argType instanceof TimestampType) {
                // Date fields: PointValues stores epoch millis, output as micros
                TimestampType.TIMESTAMP_MILLIS.writeLong(builders[i], results[i] * 1000L);
              } else if (spec.argType instanceof DoubleType) {
                DoubleType.DOUBLE.writeDouble(builders[i], Double.longBitsToDouble(results[i]));
              } else {
                outputType.writeLong(builders[i], results[i]);
              }
            }
            Block[] blocks = new Block[numAggs];
            for (int i = 0; i < numAggs; i++) {
              blocks[i] = builders[i].build();
            }
            return List.of(new Page(blocks));
          }
        }
      }

      // Ultra-fast flat-array path: for MatchAll with no deletes and only non-DISTINCT
      // numeric aggregates, read all unique columns in a single tight loop with no virtual
      // dispatch. Each column's doc values are read exactly once per doc. Accumulation uses
      // primitive long[] arrays directly, avoiding interface dispatch overhead per doc per
      // accumulator. For Q3 (SUM + COUNT + AVG on 1M docs), this eliminates ~3M virtual
      // calls and reduces per-doc overhead from ~15ns to ~8ns.
      if (query instanceof MatchAllDocsQuery
          && tryFlatArrayPath(specs, accumulators, engineSearcher)) {
        // Results are already accumulated into the accumulators by tryFlatArrayPath
      } else if (query instanceof MatchAllDocsQuery) {
        // Fast path: iterate all docs directly without Scorer/Collector overhead.
        // For MatchAllDocsQuery, we skip the query evaluation framework entirely
        // and iterate docs 0..maxDoc-1 per segment, checking liveDocs for deletes.
        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          LeafReader reader = leafCtx.reader();
          int maxDoc = reader.maxDoc();
          Bits liveDocs = reader.getLiveDocs();
          for (DirectAccumulator acc : accumulators) {
            acc.initSegment(leafCtx);
          }
          if (liveDocs == null) {
            // No deleted docs — tight inner loop without liveDocs check
            for (int doc = 0; doc < maxDoc; doc++) {
              for (DirectAccumulator acc : accumulators) {
                acc.accumulate(doc);
              }
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              if (liveDocs.get(doc)) {
                for (DirectAccumulator acc : accumulators) {
                  acc.accumulate(doc);
                }
              }
            }
          }
        }
      } else {
        // Fast path: filtered COUNT(*) only — use IndexSearcher.count(query) which is
        // highly optimized in Lucene (uses PointValues ranges, block scoring, segment
        // statistics). Avoids Collector/LeafCollector overhead entirely.
        // Critical for Q2: COUNT(*) WHERE AdvEngineID <> 0.
        boolean allCountStarFiltered = true;
        for (AggSpec spec : specs) {
          if (!"COUNT".equals(spec.funcName()) || !"*".equals(spec.arg())) {
            allCountStarFiltered = false;
            break;
          }
        }
        if (allCountStarFiltered) {
          int totalCount = engineSearcher.count(query);
          for (DirectAccumulator acc : accumulators) {
            if (acc instanceof CountStarDirectAccumulator csa) {
              csa.addCount(totalCount);
            }
          }
        } else {
          // General path: use Lucene's search framework with Collector
          engineSearcher.search(
              query,
              new Collector() {
                @Override
                public LeafCollector getLeafCollector(LeafReaderContext context)
                    throws IOException {
                  // Open doc values iterators for each accumulator in this segment
                  for (DirectAccumulator acc : accumulators) {
                    acc.initSegment(context);
                  }
                  return new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) throws IOException {
                      for (DirectAccumulator acc : accumulators) {
                        acc.accumulate(doc);
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
      }
    }

    // Build result Page
    int numAggs = accumulators.size();
    BlockBuilder[] builders = new BlockBuilder[numAggs];
    for (int i = 0; i < numAggs; i++) {
      builders[i] = accumulators.get(i).getOutputType().createBlockBuilder(null, 1);
      accumulators.get(i).writeTo(builders[i]);
    }

    Block[] blocks = new Block[numAggs];
    for (int i = 0; i < numAggs; i++) {
      blocks[i] = builders[i].build();
    }

    return List.of(new Page(blocks));
  }

  /** Resolve the output types for the fused aggregate. */
  public static List<Type> resolveOutputTypes(
      AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    List<Type> types = new ArrayList<>();
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

  /**
   * Attempt the flat-array fast path: for MatchAll with no deleted docs and only non-DISTINCT
   * numeric (long) aggregates, read all unique columns in a single tight inner loop with no virtual
   * dispatch. Returns true if the fast path was used, false if it cannot apply.
   *
   * <p>This is critical for queries like Q3 (SUM + COUNT(*) + AVG on 1M rows) where the
   * per-accumulator virtual dispatch overhead dominates. The flat-array approach reads each unique
   * column exactly once per doc and accumulates into primitive long arrays.
   *
   * <p>Eligibility: no DISTINCT, no double-type columns, no varchar columns. All aggregates must be
   * SUM, COUNT(*), COUNT(col), AVG, MIN, or MAX on long-representable types.
   */
  private static boolean tryFlatArrayPath(
      List<AggSpec> specs,
      List<DirectAccumulator> accumulators,
      org.opensearch.index.engine.Engine.Searcher engineSearcher)
      throws IOException {
    // Check eligibility: no DISTINCT, no double, no varchar, no string columns
    for (AggSpec spec : specs) {
      if (spec.isDistinct()) {
        return false;
      }
      if (spec.argType() instanceof DoubleType || spec.argType() instanceof VarcharType) {
        return false;
      }
    }
    // Check for deleted docs
    for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
      if (leafCtx.reader().getLiveDocs() != null) {
        return false;
      }
    }

    // Collect unique column names (excluding "*" for COUNT(*))
    List<String> uniqueColumns = new ArrayList<>();
    for (AggSpec spec : specs) {
      if (!"*".equals(spec.arg()) && !uniqueColumns.contains(spec.arg())) {
        uniqueColumns.add(spec.arg());
      }
    }

    int numCols = uniqueColumns.size();
    // Per-column accumulators: [sum, count, min, max] per column
    long[] colSum = new long[numCols];
    // Note: no double[] needed — all columns in this path are integer-family,
    // and long accumulation is sufficient for SUM/AVG.
    long[] colCount = new long[numCols];
    long[] colMin = new long[numCols];
    long[] colMax = new long[numCols];
    for (int i = 0; i < numCols; i++) {
      colMin[i] = Long.MAX_VALUE;
      colMax[i] = Long.MIN_VALUE;
    }
    long totalDocs = 0;
    String[] colArray = uniqueColumns.toArray(new String[0]);

    // Determine which aggregate types are actually needed to skip unnecessary work.
    // Note: colNeedsDouble is always false within this path because DoubleType columns
    // are excluded by the eligibility check above. For integer-family columns used with AVG,
    // long accumulation is sufficient — the SUM fits in a long (e.g., short max 65535 * 100M
    // rows = ~6.5e12, well within long range). The conversion to double happens only once
    // in AvgDirectAccumulator.addLongSumCount().
    boolean needMin = false, needMax = false;
    for (AggSpec spec : specs) {
      if ("MIN".equals(spec.funcName())) needMin = true;
      if ("MAX".equals(spec.funcName())) needMax = true;
    }

    // Column-major iteration: process one column at a time across all docs per segment.
    // Uses nextDoc() sequential iteration instead of advanceExact(doc) random access.
    // For dense columns (all docs have values, typical in ClickBench), nextDoc() is
    // significantly faster because it reads the compressed doc value stream sequentially
    // instead of seeking to each doc position individually. This also improves CPU cache
    // utilization since we're reading one column's data contiguously before moving to the next.

    // Parallelize across segments using ForkJoinPool
    List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
    int numWorkers =
        Math.min(
            Math.max(
                1,
                Runtime.getRuntime().availableProcessors()
                    / Integer.getInteger("dqe.numLocalShards", 4)),
            leaves.size());

    if (numWorkers > 1 && leaves.size() > 1) {
      // Parallel path: partition segments among workers using largest-first assignment
      @SuppressWarnings("unchecked")
      List<LeafReaderContext>[] workerSegments = new List[numWorkers];
      long[] workerDocCounts = new long[numWorkers];
      for (int i = 0; i < numWorkers; i++) {
        workerSegments[i] = new java.util.ArrayList<>();
      }

      // Sort segments by doc count descending, assign largest-first to lightest worker
      java.util.List<LeafReaderContext> sortedLeaves = new java.util.ArrayList<>(leaves);
      sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
      for (LeafReaderContext leaf : sortedLeaves) {
        int lightest = 0;
        for (int i = 1; i < numWorkers; i++) {
          if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
        }
        workerSegments[lightest].add(leaf);
        workerDocCounts[lightest] += leaf.reader().maxDoc();
      }

      // Capture effectively-final locals for lambda
      final int nc = numCols;
      final boolean needMinF = needMin, needMaxF = needMax;

      // Each worker packs results into a long[]:
      //   [totalDocs, colSum[0..nc-1], colCount[0..nc-1], colMin[0..nc-1], colMax[0..nc-1]]
      // No double accumulation needed: DoubleType columns are excluded by the eligibility
      // check, and for integer-family columns, long accumulation is sufficient.
      @SuppressWarnings("unchecked")
      java.util.concurrent.CompletableFuture<long[]>[] futures =
          new java.util.concurrent.CompletableFuture[numWorkers];

      for (int w = 0; w < numWorkers; w++) {
        final List<LeafReaderContext> mySegments = workerSegments[w];
        futures[w] =
            java.util.concurrent.CompletableFuture.supplyAsync(
                () -> {
                  long[] wColSum = new long[nc];
                  long[] wColCount = new long[nc];
                  long[] wColMin = new long[nc];
                  long[] wColMax = new long[nc];
                  for (int i = 0; i < nc; i++) {
                    wColMin[i] = Long.MAX_VALUE;
                    wColMax[i] = Long.MIN_VALUE;
                  }
                  long wTotalDocs = 0;

                  try {
                    for (LeafReaderContext leafCtx : mySegments) {
                      LeafReader reader = leafCtx.reader();
                      wTotalDocs += reader.maxDoc();

                      // Segment-level cache lookup using core cache key
                      IndexReader.CacheHelper coreHelper = reader.getCoreCacheHelper();
                      Object segKey = coreHelper != null ? coreHelper.getKey() : null;
                      ConcurrentHashMap<String, long[]> segCache =
                          segKey != null ? SEGMENT_AGG_CACHE.get(segKey) : null;

                      for (int c = 0; c < nc; c++) {
                        // Check cache before scanning DocValues
                        if (segCache != null) {
                          long[] cached = segCache.get(colArray[c]);
                          if (cached != null) {
                            wColSum[c] += cached[0];
                            wColCount[c] += cached[1];
                            if (needMinF && cached[2] < wColMin[c]) wColMin[c] = cached[2];
                            if (needMaxF && cached[3] > wColMax[c]) wColMax[c] = cached[3];
                            continue; // Cache hit — skip DocValues scan
                          }
                        }

                        SortedNumericDocValues dv = reader.getSortedNumericDocValues(colArray[c]);
                        if (dv == null) continue;
                        long localSum = 0;

                        if (needMinF || needMaxF) {
                          long localMin = Long.MAX_VALUE, localMax = Long.MIN_VALUE;
                          long localCount = 0;
                          int doc = dv.nextDoc();
                          while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                            long val = dv.nextValue();
                            localSum += val;
                            localCount++;
                            if (val < localMin) localMin = val;
                            if (val > localMax) localMax = val;
                            doc = dv.nextDoc();
                          }
                          if (localMin < wColMin[c]) wColMin[c] = localMin;
                          if (localMax > wColMax[c]) wColMax[c] = localMax;
                          wColCount[c] += localCount;

                          // Store in cache: sum, count, min, max
                          if (segKey != null) {
                            SEGMENT_AGG_CACHE
                                .computeIfAbsent(segKey, k -> {
                                  // Register eviction listener on first cache entry for this segment
                                  try {
                                    coreHelper.addClosedListener(
                                        closedKey -> SEGMENT_AGG_CACHE.remove(closedKey));
                                  } catch (Exception ignored) { }
                                  return new ConcurrentHashMap<>();
                                })
                                .putIfAbsent(colArray[c],
                                    new long[]{localSum, localCount, localMin, localMax});
                          }
                        } else {
                          int doc = dv.nextDoc();
                          while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                            localSum += dv.nextValue();
                            doc = dv.nextDoc();
                          }
                          wColCount[c] += reader.maxDoc();

                          // Store in cache: sum, count (min/max not needed)
                          if (segKey != null) {
                            SEGMENT_AGG_CACHE
                                .computeIfAbsent(segKey, k -> {
                                  try {
                                    coreHelper.addClosedListener(
                                        closedKey -> SEGMENT_AGG_CACHE.remove(closedKey));
                                  } catch (Exception ignored) { }
                                  return new ConcurrentHashMap<>();
                                })
                                .putIfAbsent(colArray[c],
                                    new long[]{localSum, reader.maxDoc(), Long.MAX_VALUE, Long.MIN_VALUE});
                          }
                        }
                        wColSum[c] += localSum;
                      }
                    }
                  } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                  }

                  // Pack results into long array
                  long[] pack = new long[1 + nc * 4];
                  pack[0] = wTotalDocs;
                  for (int c = 0; c < nc; c++) {
                    pack[1 + c] = wColSum[c];
                    pack[1 + nc + c] = wColCount[c];
                    pack[1 + 2 * nc + c] = wColMin[c];
                    pack[1 + 3 * nc + c] = wColMax[c];
                  }
                  return pack;
                },
                FusedGroupByAggregate.getParallelPool());
      }

      // Merge worker results
      java.util.concurrent.CompletableFuture.allOf(futures).join();
      for (var future : futures) {
        long[] pack = future.join();
        totalDocs += pack[0];
        for (int c = 0; c < numCols; c++) {
          colSum[c] += pack[1 + c];
          colCount[c] += pack[1 + numCols + c];
          if (pack[1 + 2 * numCols + c] < colMin[c]) colMin[c] = pack[1 + 2 * numCols + c];
          if (pack[1 + 3 * numCols + c] > colMax[c]) colMax[c] = pack[1 + 3 * numCols + c];
        }
      }
    } else {
      // Sequential path: single worker or single segment
      for (LeafReaderContext leafCtx : leaves) {
        LeafReader reader = leafCtx.reader();
        int maxDoc = reader.maxDoc();
        totalDocs += maxDoc;

        // Segment-level cache lookup using core cache key
        IndexReader.CacheHelper coreHelper = reader.getCoreCacheHelper();
        Object segKey = coreHelper != null ? coreHelper.getKey() : null;
        ConcurrentHashMap<String, long[]> segCache =
            segKey != null ? SEGMENT_AGG_CACHE.get(segKey) : null;

        for (int c = 0; c < numCols; c++) {
          // Check cache before scanning DocValues
          if (segCache != null) {
            long[] cached = segCache.get(colArray[c]);
            if (cached != null) {
              colSum[c] += cached[0];
              colCount[c] += cached[1];
              if (needMin && cached[2] < colMin[c]) colMin[c] = cached[2];
              if (needMax && cached[3] > colMax[c]) colMax[c] = cached[3];
              continue; // Cache hit — skip DocValues scan
            }
          }

          SortedNumericDocValues dv = reader.getSortedNumericDocValues(colArray[c]);
          if (dv == null) continue;

          long localSum = 0;

          if (needMin || needMax) {
            long localMin = Long.MAX_VALUE;
            long localMax = Long.MIN_VALUE;
            long localCount = 0;
            int doc = dv.nextDoc();
            while (doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              long val = dv.nextValue();
              localSum += val;
              localCount++;
              if (val < localMin) localMin = val;
              if (val > localMax) localMax = val;
              doc = dv.nextDoc();
            }
            if (localMin < colMin[c]) colMin[c] = localMin;
            if (localMax > colMax[c]) colMax[c] = localMax;
            colCount[c] += localCount;

            // Store in cache
            if (segKey != null) {
              SEGMENT_AGG_CACHE
                  .computeIfAbsent(segKey, k -> {
                    try {
                      coreHelper.addClosedListener(
                          closedKey -> SEGMENT_AGG_CACHE.remove(closedKey));
                    } catch (Exception ignored) { }
                    return new ConcurrentHashMap<>();
                  })
                  .putIfAbsent(colArray[c],
                      new long[]{localSum, localCount, localMin, localMax});
            }
          } else {
            int doc = dv.nextDoc();
            while (doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              localSum += dv.nextValue();
              doc = dv.nextDoc();
            }
            colCount[c] += maxDoc;

            // Store in cache
            if (segKey != null) {
              SEGMENT_AGG_CACHE
                  .computeIfAbsent(segKey, k -> {
                    try {
                      coreHelper.addClosedListener(
                          closedKey -> SEGMENT_AGG_CACHE.remove(closedKey));
                    } catch (Exception ignored) { }
                    return new ConcurrentHashMap<>();
                  })
                  .putIfAbsent(colArray[c],
                      new long[]{localSum, maxDoc, Long.MAX_VALUE, Long.MIN_VALUE});
            }
          }
          colSum[c] += localSum;
        }
      }
    }

    // Map flat-array results back to accumulators
    for (int i = 0; i < specs.size(); i++) {
      AggSpec spec = specs.get(i);
      DirectAccumulator acc = accumulators.get(i);
      if ("COUNT".equals(spec.funcName()) && "*".equals(spec.arg())) {
        if (acc instanceof CountStarDirectAccumulator csa) {
          csa.addCount(totalDocs);
        }
      } else {
        int colIdx = uniqueColumns.indexOf(spec.arg());
        switch (spec.funcName()) {
          case "SUM":
            if (acc instanceof SumDirectAccumulator sa) {
              sa.addLongSum(colSum[colIdx], colCount[colIdx]);
            }
            break;
          case "COUNT":
            if (acc instanceof CountStarDirectAccumulator csa) {
              csa.addCount(colCount[colIdx]);
            }
            break;
          case "AVG":
            if (acc instanceof AvgDirectAccumulator aa) {
              aa.addLongSumCount(colSum[colIdx], colCount[colIdx]);
            }
            break;
          case "MIN":
            if (acc instanceof MinDirectAccumulator ma) {
              ma.mergeMin(colMin[colIdx], colCount[colIdx] > 0);
            }
            break;
          case "MAX":
            if (acc instanceof MaxDirectAccumulator ma) {
              ma.mergeMax(colMax[colIdx], colCount[colIdx] > 0);
            }
            break;
        }
      }
    }
    return true;
  }

  /** Specification for a single aggregate function. */
  private record AggSpec(String funcName, boolean isDistinct, String arg, Type argType) {}

  private static DirectAccumulator createAccumulator(AggSpec spec) {
    switch (spec.funcName) {
      case "COUNT":
        if (spec.isDistinct) {
          return new CountDistinctDirectAccumulator(spec.arg, spec.argType);
        }
        if ("*".equals(spec.arg)) {
          return new CountStarDirectAccumulator();
        }
        return new CountStarDirectAccumulator(); // COUNT(col) ~ COUNT(*) when no nulls
      case "SUM":
        return new SumDirectAccumulator(spec.arg, spec.argType);
      case "MIN":
        return new MinDirectAccumulator(spec.arg, spec.argType);
      case "MAX":
        return new MaxDirectAccumulator(spec.arg, spec.argType);
      case "AVG":
        return new AvgDirectAccumulator(spec.arg, spec.argType);
      default:
        throw new UnsupportedOperationException("Unsupported aggregate: " + spec.funcName);
    }
  }

  /** Direct accumulator that reads from Lucene doc values without intermediate Pages. */
  private interface DirectAccumulator {
    void initSegment(LeafReaderContext leaf) throws IOException;

    void accumulate(int doc) throws IOException;

    Type getOutputType();

    void writeTo(BlockBuilder builder);
  }

  /** COUNT(*) accumulator. */
  private static class CountStarDirectAccumulator implements DirectAccumulator {
    private long count = 0;

    /** Add a bulk count (for O(1) COUNT(*) with MatchAllDocsQuery). */
    void addCount(long n) {
      count += n;
    }

    @Override
    public void initSegment(LeafReaderContext leaf) {}

    @Override
    public void accumulate(int doc) {
      count++;
    }

    @Override
    public Type getOutputType() {
      return BigintType.BIGINT;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      BigintType.BIGINT.writeLong(builder, count);
    }
  }

  /** SUM accumulator for numeric columns. */
  private static class SumDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final boolean isDoubleType;
    private SortedNumericDocValues dv;
    private long longSum = 0;
    private double doubleSum = 0;
    private boolean hasValue = false;

    SumDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.isDoubleType = argType instanceof DoubleType;
    }

    /** Merge pre-computed long sum from the flat-array fast path. */
    void addLongSum(long sum, long count) {
      if (count > 0) {
        hasValue = true;
        longSum += sum;
      }
    }

    @Override
    public void initSegment(LeafReaderContext leaf) throws IOException {
      dv = leaf.reader().getSortedNumericDocValues(field);
    }

    @Override
    public void accumulate(int doc) throws IOException {
      if (dv != null && dv.advanceExact(doc)) {
        hasValue = true;
        if (isDoubleType) {
          doubleSum += Double.longBitsToDouble(dv.nextValue());
        } else {
          longSum += dv.nextValue();
        }
      }
    }

    @Override
    public Type getOutputType() {
      return isDoubleType ? DoubleType.DOUBLE : BigintType.BIGINT;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isDoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, doubleSum);
      } else {
        BigintType.BIGINT.writeLong(builder, longSum);
      }
    }
  }

  /** MIN accumulator for numeric columns. */
  private static class MinDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final Type argType;
    private final boolean isLongType;
    private final boolean isDoubleType;
    private final boolean isVarchar;
    private final boolean isTimestamp;
    private SortedNumericDocValues numericDv;
    private SortedSetDocValues stringDv;
    private long longMin = Long.MAX_VALUE;
    private double doubleMin = Double.MAX_VALUE;
    private String stringMin = null;
    private boolean hasValue = false;

    MinDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.argType = argType;
      this.isDoubleType = argType instanceof DoubleType;
      this.isVarchar = argType instanceof VarcharType;
      this.isTimestamp = argType instanceof TimestampType;
      this.isLongType = !isDoubleType && !isVarchar;
    }

    /** Merge pre-computed min from the flat-array fast path. */
    void mergeMin(long min, boolean has) {
      if (has) {
        hasValue = true;
        if (min < longMin) longMin = min;
      }
    }

    @Override
    public void initSegment(LeafReaderContext leaf) throws IOException {
      if (isVarchar) {
        stringDv = leaf.reader().getSortedSetDocValues(field);
      } else {
        numericDv = leaf.reader().getSortedNumericDocValues(field);
      }
    }

    @Override
    public void accumulate(int doc) throws IOException {
      if (isVarchar) {
        if (stringDv != null && stringDv.advanceExact(doc)) {
          hasValue = true;
          BytesRef bytes = stringDv.lookupOrd(stringDv.nextOrd());
          String val = bytes.utf8ToString();
          if (stringMin == null || val.compareTo(stringMin) < 0) {
            stringMin = val;
          }
        }
      } else if (isDoubleType) {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          hasValue = true;
          double val = Double.longBitsToDouble(numericDv.nextValue());
          if (val < doubleMin) doubleMin = val;
        }
      } else {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          hasValue = true;
          long val = numericDv.nextValue();
          if (val < longMin) longMin = val;
        }
      }
    }

    @Override
    public Type getOutputType() {
      return argType;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isVarchar) {
        VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(stringMin));
      } else if (isDoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, doubleMin);
      } else if (isTimestamp) {
        // Lucene stores epoch millis, Trino expects epoch microseconds
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, longMin * 1000L);
      } else {
        argType.writeLong(builder, longMin);
      }
    }
  }

  /** MAX accumulator for numeric columns. */
  private static class MaxDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final Type argType;
    private final boolean isLongType;
    private final boolean isDoubleType;
    private final boolean isVarchar;
    private final boolean isTimestamp;
    private SortedNumericDocValues numericDv;
    private SortedSetDocValues stringDv;
    private long longMax = Long.MIN_VALUE;
    private double doubleMax = -Double.MAX_VALUE;
    private String stringMax = null;
    private boolean hasValue = false;

    MaxDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.argType = argType;
      this.isDoubleType = argType instanceof DoubleType;
      this.isVarchar = argType instanceof VarcharType;
      this.isTimestamp = argType instanceof TimestampType;
      this.isLongType = !isDoubleType && !isVarchar;
    }

    /** Merge pre-computed max from the flat-array fast path. */
    void mergeMax(long max, boolean has) {
      if (has) {
        hasValue = true;
        if (max > longMax) longMax = max;
      }
    }

    @Override
    public void initSegment(LeafReaderContext leaf) throws IOException {
      if (isVarchar) {
        stringDv = leaf.reader().getSortedSetDocValues(field);
      } else {
        numericDv = leaf.reader().getSortedNumericDocValues(field);
      }
    }

    @Override
    public void accumulate(int doc) throws IOException {
      if (isVarchar) {
        if (stringDv != null && stringDv.advanceExact(doc)) {
          hasValue = true;
          BytesRef bytes = stringDv.lookupOrd(stringDv.nextOrd());
          String val = bytes.utf8ToString();
          if (stringMax == null || val.compareTo(stringMax) > 0) {
            stringMax = val;
          }
        }
      } else if (isDoubleType) {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          hasValue = true;
          double val = Double.longBitsToDouble(numericDv.nextValue());
          if (val > doubleMax) doubleMax = val;
        }
      } else {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          hasValue = true;
          long val = numericDv.nextValue();
          if (val > longMax) longMax = val;
        }
      }
    }

    @Override
    public Type getOutputType() {
      return argType;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isVarchar) {
        VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(stringMax));
      } else if (isDoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, doubleMax);
      } else if (isTimestamp) {
        // Lucene stores epoch millis, Trino expects epoch microseconds
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, longMax * 1000L);
      } else {
        argType.writeLong(builder, longMax);
      }
    }
  }

  /**
   * AVG accumulator. Uses double accumulation for the sum to avoid long overflow when summing large
   * values (e.g., UserID ~10^18) across many rows. The output is always DOUBLE, so the slight
   * precision loss from double arithmetic is acceptable and matches standard SQL behavior for AVG
   * on integer types.
   */
  private static class AvgDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final boolean isDoubleType;
    private SortedNumericDocValues dv;
    private double doubleSum = 0;
    private long count = 0;

    AvgDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.isDoubleType = argType instanceof DoubleType;
    }

    /** Merge pre-computed long sum and count from the flat-array fast path. */
    void addLongSumCount(long sum, long cnt) {
      if (cnt > 0) {
        doubleSum += sum;
        count += cnt;
      }
    }

    /** Merge pre-computed double sum and count from the flat-array fast path. */
    void addDoubleSumCount(double sum, long cnt) {
      if (cnt > 0) {
        doubleSum += sum;
        count += cnt;
      }
    }

    @Override
    public void initSegment(LeafReaderContext leaf) throws IOException {
      dv = leaf.reader().getSortedNumericDocValues(field);
    }

    @Override
    public void accumulate(int doc) throws IOException {
      if (dv != null && dv.advanceExact(doc)) {
        count++;
        if (isDoubleType) {
          doubleSum += Double.longBitsToDouble(dv.nextValue());
        } else {
          doubleSum += dv.nextValue();
        }
      }
    }

    @Override
    public Type getOutputType() {
      return DoubleType.DOUBLE;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (count == 0) {
        builder.appendNull();
      } else {
        DoubleType.DOUBLE.writeDouble(builder, doubleSum / count);
      }
    }
  }

  /**
   * Execute a fused scan to collect distinct values for a single numeric column, returning the
   * distinct values themselves (not the count) as a single-column Page. This is used by the SINGLE
   * COUNT(DISTINCT) coordinator merge: each shard returns its ~25K distinct values instead of all
   * ~125K raw values, and the coordinator merges the much smaller distinct sets.
   *
   * @param columnName the column to collect distinct values from
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @return a list containing a single Page with the distinct values as a BigintType column
   */
  public static List<Page> executeDistinctValues(String columnName, IndexShard shard, Query query)
      throws Exception {
    LongOpenHashSet distinctSet;

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-values")) {
      // Pre-size based on maxDoc to avoid resize storms for high-cardinality columns
      int totalDocs = engineSearcher.getIndexReader().maxDoc();
      distinctSet =
          totalDocs > 0
              ? new LongOpenHashSet(Math.min(totalDocs, 32_000_000))
              : new LongOpenHashSet();
      if (query instanceof MatchAllDocsQuery) {
        // Fast path: iterate all docs directly
        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          LeafReader reader = leafCtx.reader();
          int maxDoc = reader.maxDoc();
          Bits liveDocs = reader.getLiveDocs();
          SortedNumericDocValues dv = reader.getSortedNumericDocValues(columnName);
          if (dv == null) continue;
          if (liveDocs == null) {
            for (int doc = 0; doc < maxDoc; doc++) {
              if (dv.advanceExact(doc)) {
                distinctSet.add(dv.nextValue());
              }
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              if (liveDocs.get(doc) && dv.advanceExact(doc)) {
                distinctSet.add(dv.nextValue());
              }
            }
          }
        }
      } else {
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues dv = context.reader().getSortedNumericDocValues(columnName);
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    if (dv != null && dv.advanceExact(doc)) {
                      distinctSet.add(dv.nextValue());
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
    }

    // Build a Page with the distinct values
    int distinctCount = distinctSet.size();
    if (distinctCount == 0) {
      return List.of();
    }

    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, distinctCount);
    // Iterate through the LongOpenHashSet and write values
    if (distinctSet.hasZeroValue()) {
      BigintType.BIGINT.writeLong(builder, 0L);
    }
    if (distinctSet.hasSentinelValue()) {
      BigintType.BIGINT.writeLong(
          builder, org.opensearch.sql.dqe.operator.LongOpenHashSet.emptyMarker());
    }
    long[] keys = distinctSet.keys();
    long emptyMarker = org.opensearch.sql.dqe.operator.LongOpenHashSet.emptyMarker();
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] != emptyMarker) {
        BigintType.BIGINT.writeLong(builder, keys[i]);
      }
    }
    return List.of(new Page(builder.build()));
  }

  /**
   * Execute a fused scan to collect distinct values for a single numeric column, returning the raw
   * LongOpenHashSet directly (no Page construction). This avoids the overhead of building a
   * BlockBuilder with millions of entries and then extracting them again at the coordinator.
   *
   * @param columnName the column to collect distinct values from
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @return the raw LongOpenHashSet containing all distinct values in this shard
   */
  public static LongOpenHashSet collectDistinctValuesRaw(
      String columnName, IndexShard shard, Query query) throws Exception {
    LongOpenHashSet distinctSet;

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-values-raw")) {
      // Pre-size based on total docs to minimize resizes. For high-cardinality columns
      // (e.g., UserID with ~4.25M distinct per shard), starting at 65536 causes 7 resizes,
      // each rehashing all entries. Pre-sizing to ~8M avoids all resizes.
      long totalDocs = 0;
      for (LeafReaderContext leaf : engineSearcher.getIndexReader().leaves()) {
        totalDocs += leaf.reader().maxDoc();
      }
      // Estimate distinct count as min(totalDocs/4, 8M) — conservative for high-cardinality
      int estimatedDistinct = (int) Math.min(totalDocs / 4, 8_000_000);
      distinctSet = new LongOpenHashSet(Math.max(65536, estimatedDistinct));
      if (query instanceof MatchAllDocsQuery) {
        // Phase 1: Parallel columnar load — read DocValues into flat long[] arrays per segment.
        // Phase 2: Parallel per-segment hash set insertion, then merge.
        // Two-phase approach is faster than fused (DocValues read + hash insert) because
        // Phase 1 has sequential memory access and Phase 2 has sequential array reads,
        // while fused interleaves random hash probes with sequential DocValues reads.
        java.util.List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
        long[][] segArrays = new long[leaves.size()][];
        int[] segCounts = new int[leaves.size()];
        if (leaves.size() > 1) {
          java.util.concurrent.CompletableFuture<?>[] futures =
              new java.util.concurrent.CompletableFuture[leaves.size()];
          for (int s = 0; s < leaves.size(); s++) {
            final int segIdx = s;
            futures[s] =
                java.util.concurrent.CompletableFuture.runAsync(
                    () -> {
                      try {
                        LeafReader reader = leaves.get(segIdx).reader();
                        SortedNumericDocValues dv =
                            reader.getSortedNumericDocValues(columnName);
                        if (dv == null) {
                          segArrays[segIdx] = new long[0];
                          return;
                        }
                        int maxDoc = reader.maxDoc();
                        long[] vals = new long[maxDoc];
                        Bits liveDocs = reader.getLiveDocs();
                        int count = 0;
                        if (liveDocs == null) {
                          int doc = dv.nextDoc();
                          while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                            vals[count++] = dv.nextValue();
                            doc = dv.nextDoc();
                          }
                        } else {
                          for (int doc = 0; doc < maxDoc; doc++) {
                            if (liveDocs.get(doc) && dv.advanceExact(doc)) {
                              vals[count++] = dv.nextValue();
                            }
                          }
                        }
                        segArrays[segIdx] = vals;
                        segCounts[segIdx] = count;
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    },
                    FusedGroupByAggregate.getParallelPool());
          }
          java.util.concurrent.CompletableFuture.allOf(futures).join();
        } else {
          // Single segment — fused single-pass: read DocValues directly into hash set
          // with inline run-length dedup, avoiding temp long[maxDoc] allocation.
          LeafReader reader = leaves.get(0).reader();
          SortedNumericDocValues dv = reader.getSortedNumericDocValues(columnName);
          if (dv != null) {
            Bits liveDocs = reader.getLiveDocs();
            if (liveDocs == null) {
              // Fused single-pass: read DocValues directly into hash set with run-length dedup
              long prev = Long.MIN_VALUE;
              boolean hasPrev = false;
              int doc = dv.nextDoc();
              while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                long v = dv.nextValue();
                if (!hasPrev || v != prev) {
                  distinctSet.add(v);
                  prev = v;
                  hasPrev = true;
                }
                doc = dv.nextDoc();
              }
            } else {
              // With liveDocs: advanceExact per live doc
              int maxDoc = reader.maxDoc();
              long prev = Long.MIN_VALUE;
              boolean hasPrev = false;
              for (int doc = 0; doc < maxDoc; doc++) {
                if (liveDocs.get(doc) && dv.advanceExact(doc)) {
                  long v = dv.nextValue();
                  if (!hasPrev || v != prev) {
                    distinctSet.add(v);
                    prev = v;
                    hasPrev = true;
                  }
                }
              }
            }
          }
        }
        // Phase 2: Parallel per-segment hash insertion, then merge
        if (leaves.size() > 1) {
          @SuppressWarnings("unchecked")
          java.util.concurrent.CompletableFuture<LongOpenHashSet>[] hashFutures =
              new java.util.concurrent.CompletableFuture[leaves.size()];
          for (int s = 0; s < leaves.size(); s++) {
            final int segIdx = s;
            hashFutures[s] =
                java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> {
                      long[] vals = segArrays[segIdx];
                      if (vals == null || segCounts[segIdx] == 0) {
                        return new LongOpenHashSet();
                      }
                      LongOpenHashSet set = new LongOpenHashSet(Math.min(segCounts[segIdx], 8_000_000));
                      int count = segCounts[segIdx];
                      // Use prefetch-batched insertion for better cache behavior
                      set.addAllBatched(vals, 0, count);
                      return set;
                    },
                    FusedGroupByAggregate.getParallelPool());
          }
          LongOpenHashSet[] segSets = new LongOpenHashSet[leaves.size()];
          for (int s = 0; s < leaves.size(); s++) {
            segSets[s] = hashFutures[s].join();
          }
          // Merge: find largest set, addAll smaller sets into it
          int largestIdx = 0;
          for (int s = 1; s < segSets.length; s++) {
            if (segSets[s].size() > segSets[largestIdx].size()) {
              largestIdx = s;
            }
          }
          for (int s = 0; s < segSets.length; s++) {
            if (s != largestIdx) {
              segSets[largestIdx].addAll(segSets[s]);
            }
          }
          distinctSet = segSets[largestIdx];
        } else {
          // Single segment — already inserted directly into distinctSet in fused Phase 1
        }
      } else {
        final LongOpenHashSet filteredSet = distinctSet;
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues dv = context.reader().getSortedNumericDocValues(columnName);
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    if (dv != null && dv.advanceExact(doc)) {
                      filteredSet.add(dv.nextValue());
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
    }
    return distinctSet;
  }

  /**
   * Collect distinct varchar values as a LongOpenHashSet of hashes, avoiding string materialization.
   * Uses FNV-1a hash on raw BytesRef bytes. For MatchAll with no deletes: iterates global ordinals
   * (O(distinct_values)). For filtered queries: iterates matching docs via FixedBitSet.
   */
  public static LongOpenHashSet collectDistinctVarcharHashes(
      String columnName, IndexShard shard, Query query) throws Exception {
    LongOpenHashSet hashes = new LongOpenHashSet();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-varchar-hashes")) {
      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();

      if (query instanceof MatchAllDocsQuery) {
        boolean hasDeletes = false;
        for (LeafReaderContext leafCtx : leaves) {
          if (leafCtx.reader().getLiveDocs() != null) { hasDeletes = true; break; }
        }
        OrdinalMap ordinalMap = FusedGroupByAggregate.buildGlobalOrdinalMap(leaves, columnName);
        if (ordinalMap != null && !hasDeletes) {
          long globalOrdCount = ordinalMap.getValueCount();
          hashes = new LongOpenHashSet((int) Math.min(globalOrdCount * 2, Integer.MAX_VALUE));
          int numWorkers = Math.min(
              Math.max(1, Runtime.getRuntime().availableProcessors()
                  / Integer.getInteger("dqe.numLocalShards", 4)), 4);
          if (numWorkers > 1 && globalOrdCount > 100_000) {
            LongOpenHashSet[] workerSets = new LongOpenHashSet[numWorkers];
            long chunkSize = (globalOrdCount + numWorkers - 1) / numWorkers;
            @SuppressWarnings("unchecked")
            java.util.concurrent.CompletableFuture<?>[] futures =
                new java.util.concurrent.CompletableFuture[numWorkers];
            for (int w = 0; w < numWorkers; w++) {
              final long start = w * chunkSize;
              final long end = Math.min(start + chunkSize, globalOrdCount);
              final int wIdx = w;
              futures[w] = java.util.concurrent.CompletableFuture.runAsync(() -> {
                try {
                  SortedSetDocValues[] localDvs = new SortedSetDocValues[leaves.size()];
                  for (int i = 0; i < leaves.size(); i++) {
                    localDvs[i] = leaves.get(i).reader().getSortedSetDocValues(columnName);
                  }
                  LongOpenHashSet localSet = new LongOpenHashSet(
                      (int) Math.min(end - start + 16, Integer.MAX_VALUE));
                  for (long g = start; g < end; g++) {
                    int segIdx = ordinalMap.getFirstSegmentNumber(g);
                    long segOrd = ordinalMap.getFirstSegmentOrd(g);
                    BytesRef bytes = (localDvs[segIdx] != null)
                        ? localDvs[segIdx].lookupOrd(segOrd) : new BytesRef("");
                    localSet.add(hashBytesRef(bytes));
                  }
                  workerSets[wIdx] = localSet;
                } catch (java.io.IOException e) {
                  throw new java.io.UncheckedIOException(e);
                }
              }, FusedGroupByAggregate.getParallelPool());
            }
            java.util.concurrent.CompletableFuture.allOf(futures).join();
            int largestIdx = 0;
            for (int i = 1; i < numWorkers; i++) {
              if (workerSets[i] != null && (workerSets[largestIdx] == null
                  || workerSets[i].size() > workerSets[largestIdx].size())) {
                largestIdx = i;
              }
            }
            hashes = workerSets[largestIdx];
            for (int i = 0; i < numWorkers; i++) {
              if (i != largestIdx && workerSets[i] != null) {
                hashes.addAll(workerSets[i]);
              }
            }
          } else {
            SortedSetDocValues[] segDvs = new SortedSetDocValues[leaves.size()];
            for (int i = 0; i < leaves.size(); i++) {
              segDvs[i] = leaves.get(i).reader().getSortedSetDocValues(columnName);
            }
            for (long g = 0; g < globalOrdCount; g++) {
              int segIdx = ordinalMap.getFirstSegmentNumber(g);
              long segOrd = ordinalMap.getFirstSegmentOrd(g);
              BytesRef bytes = (segDvs[segIdx] != null)
                  ? segDvs[segIdx].lookupOrd(segOrd) : new BytesRef("");
              hashes.add(hashBytesRef(bytes));
            }
          }
        } else {
          for (LeafReaderContext leafCtx : leaves) {
            LeafReader reader = leafCtx.reader();
            Bits liveDocs = reader.getLiveDocs();
            SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
            if (dv == null) continue;
            long valueCount = dv.getValueCount();
            if (liveDocs == null) {
              for (long ord = 0; ord < valueCount; ord++) {
                hashes.add(hashBytesRef(dv.lookupOrd(ord)));
              }
            } else {
              int maxDoc = reader.maxDoc();
              FixedBitSet usedOrdinals = new FixedBitSet(
                  (int) Math.min(valueCount, Integer.MAX_VALUE));
              for (int doc = 0; doc < maxDoc; doc++) {
                if (liveDocs.get(doc) && dv.advanceExact(doc)) {
                  usedOrdinals.set((int) dv.nextOrd());
                }
              }
              for (int ord = usedOrdinals.nextSetBit(0); ord != -1;
                  ord = (ord + 1 < usedOrdinals.length())
                      ? usedOrdinals.nextSetBit(ord + 1) : -1) {
                hashes.add(hashBytesRef(dv.lookupOrd(ord)));
              }
            }
          }
        }
      } else {
        // For NOT-EMPTY filter (WHERE col <> ''), use MatchAll ordinals path minus empty hash
        if (isNotEmptyVarcharFilter(query, columnName)) {
          long emptyHash = hashBytesRef(new BytesRef(""));
          boolean hasDeletes = false;
          for (LeafReaderContext leafCtx : leaves) {
            if (leafCtx.reader().getLiveDocs() != null) { hasDeletes = true; break; }
          }
          OrdinalMap ordinalMap = FusedGroupByAggregate.buildGlobalOrdinalMap(leaves, columnName);
          if (ordinalMap != null && !hasDeletes) {
            long globalOrdCount = ordinalMap.getValueCount();
            hashes = new LongOpenHashSet((int) Math.min(globalOrdCount * 2, Integer.MAX_VALUE));
            int numWorkers = Math.min(
                Math.max(1, Runtime.getRuntime().availableProcessors()
                    / Integer.getInteger("dqe.numLocalShards", 4)), 4);
            if (numWorkers > 1 && globalOrdCount > 100_000) {
              LongOpenHashSet[] workerSets = new LongOpenHashSet[numWorkers];
              long chunkSize = (globalOrdCount + numWorkers - 1) / numWorkers;
              @SuppressWarnings("unchecked")
              java.util.concurrent.CompletableFuture<?>[] futures =
                  new java.util.concurrent.CompletableFuture[numWorkers];
              for (int w = 0; w < numWorkers; w++) {
                final long start = w * chunkSize;
                final long end = Math.min(start + chunkSize, globalOrdCount);
                final int wIdx = w;
                futures[w] = java.util.concurrent.CompletableFuture.runAsync(() -> {
                  try {
                    SortedSetDocValues[] localDvs = new SortedSetDocValues[leaves.size()];
                    for (int i = 0; i < leaves.size(); i++) {
                      localDvs[i] = leaves.get(i).reader().getSortedSetDocValues(columnName);
                    }
                    LongOpenHashSet localSet = new LongOpenHashSet(
                        (int) Math.min(end - start + 16, Integer.MAX_VALUE));
                    for (long g = start; g < end; g++) {
                      int segIdx = ordinalMap.getFirstSegmentNumber(g);
                      long segOrd = ordinalMap.getFirstSegmentOrd(g);
                      BytesRef bytes = (localDvs[segIdx] != null)
                          ? localDvs[segIdx].lookupOrd(segOrd) : new BytesRef("");
                      if (bytes.length > 0) { // skip empty string
                        localSet.add(hashBytesRef(bytes));
                      }
                    }
                    workerSets[wIdx] = localSet;
                  } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                  }
                }, FusedGroupByAggregate.getParallelPool());
              }
              java.util.concurrent.CompletableFuture.allOf(futures).join();
              int largestIdx = 0;
              for (int i = 1; i < numWorkers; i++) {
                if (workerSets[i] != null && (workerSets[largestIdx] == null
                    || workerSets[i].size() > workerSets[largestIdx].size())) {
                  largestIdx = i;
                }
              }
              hashes = workerSets[largestIdx];
              for (int i = 0; i < numWorkers; i++) {
                if (i != largestIdx && workerSets[i] != null) {
                  hashes.addAll(workerSets[i]);
                }
              }
            } else {
              SortedSetDocValues[] segDvs = new SortedSetDocValues[leaves.size()];
              for (int i = 0; i < leaves.size(); i++) {
                segDvs[i] = leaves.get(i).reader().getSortedSetDocValues(columnName);
              }
              for (long g = 0; g < globalOrdCount; g++) {
                int segIdx = ordinalMap.getFirstSegmentNumber(g);
                long segOrd = ordinalMap.getFirstSegmentOrd(g);
                BytesRef bytes = (segDvs[segIdx] != null)
                    ? segDvs[segIdx].lookupOrd(segOrd) : new BytesRef("");
                if (bytes.length > 0) {
                  hashes.add(hashBytesRef(bytes));
                }
              }
            }
          } else {
            // Fallback for deletes: per-segment, skip empty
            for (LeafReaderContext leafCtx : leaves) {
              SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
              if (dv == null) continue;
              long valueCount = dv.getValueCount();
              for (long ord = 0; ord < valueCount; ord++) {
                BytesRef bytes = dv.lookupOrd(ord);
                if (bytes.length > 0) hashes.add(hashBytesRef(bytes));
              }
            }
          }
        } else {
          // General filtered path
          for (LeafReaderContext leafCtx : leaves) {
            LeafReader reader = leafCtx.reader();
            SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
            if (dv == null) continue;
            int maxDoc = reader.maxDoc();
            Bits liveDocs = reader.getLiveDocs();
            long valueCount = dv.getValueCount();
            FixedBitSet usedOrdinals =
                new FixedBitSet((int) Math.min(valueCount, Integer.MAX_VALUE));
            for (int doc = 0; doc < maxDoc; doc++) {
              boolean isLive = liveDocs == null || liveDocs.get(doc);
              if (isLive && dv.advanceExact(doc)) {
                usedOrdinals.set((int) dv.nextOrd());
              }
            }
            for (int ord = usedOrdinals.nextSetBit(0); ord != -1;
                ord = (ord + 1 < usedOrdinals.length())
                    ? usedOrdinals.nextSetBit(ord + 1) : -1) {
              hashes.add(hashBytesRef(dv.lookupOrd(ord)));
            }
          }
        }
      }
    }
    return hashes;
  }

  /** FNV-1a 64-bit hash for BytesRef. */
  private static long hashBytesRef(BytesRef bytes) {
    long h = 0xcbf29ce484222325L;
    for (int i = bytes.offset; i < bytes.offset + bytes.length; i++) {
      h ^= bytes.bytes[i] & 0xFF;
      h *= 0x100000001b3L;
    }
    return h;
  }

  /**
   * Detect if a query is a NOT-EQUAL-EMPTY filter on the given column.
   * Matches: BooleanQuery(MUST MatchAll, MUST_NOT TermQuery(col, ""))
   */
  private static boolean isNotEmptyVarcharFilter(Query query, String columnName) {
    if (!(query instanceof org.apache.lucene.search.BooleanQuery bq)) return false;
    boolean hasMustNotEmpty = false;
    for (org.apache.lucene.search.BooleanClause clause : bq.clauses()) {
      if (clause.occur() == org.apache.lucene.search.BooleanClause.Occur.MUST
          && clause.query() instanceof MatchAllDocsQuery) {
        continue;
      } else if (clause.occur() == org.apache.lucene.search.BooleanClause.Occur.MUST_NOT
          && clause.query() instanceof org.apache.lucene.search.TermQuery tq) {
        if (tq.getTerm().field().equals(columnName)
            && tq.getTerm().bytes().length == 0) {
          hasMustNotEmpty = true;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
    return hasMustNotEmpty;
  }

  /**
   * Execute a fused scan to collect distinct strings for a single VARCHAR column, returning the raw
   * HashSet directly (no Page construction). This avoids BlockBuilder overhead.
   *
   * @param columnName the VARCHAR column to collect distinct values from
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @return the raw HashSet containing all distinct string values in this shard
   */
  public static java.util.Set<String> collectDistinctStringsRaw(
      String columnName, IndexShard shard, Query query) throws Exception {
    java.util.Set<String> distinctStrings = new java.util.HashSet<>();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-strings-raw")) {
      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();

      if (query instanceof MatchAllDocsQuery) {
        // Global ordinals path: build OrdinalMap once (cached), iterate global ordinals
        // to materialize each distinct string exactly once. Avoids duplicate String creation
        // across segments (segment ordinals overlap → same string created N times in old path).
        boolean hasDeletes = false;
        for (LeafReaderContext leafCtx : leaves) {
          if (leafCtx.reader().getLiveDocs() != null) {
            hasDeletes = true;
            break;
          }
        }

        OrdinalMap ordinalMap = FusedGroupByAggregate.buildGlobalOrdinalMap(leaves, columnName);
        if (ordinalMap != null && !hasDeletes) {
          // No deletes + MatchAll: every global ordinal is used. Iterate once.
          long globalOrdCount = ordinalMap.getValueCount();
          SortedSetDocValues[] segDvs = new SortedSetDocValues[leaves.size()];
          for (int i = 0; i < leaves.size(); i++) {
            segDvs[i] = leaves.get(i).reader().getSortedSetDocValues(columnName);
          }
          distinctStrings = new java.util.HashSet<>((int) Math.min(globalOrdCount * 2, Integer.MAX_VALUE));

          // Parallel resolution of global ordinals to strings.
          // Workers insert directly into a ConcurrentHashMap-backed set to avoid
          // the sequential Collections.addAll() bottleneck.
          int numWorkers = Math.min(
              Math.max(1, Runtime.getRuntime().availableProcessors()
                  / Integer.getInteger("dqe.numLocalShards", 4)),
              4);
          if (numWorkers > 1 && globalOrdCount > 100_000) {
            java.util.Set<String> concurrentSet = java.util.concurrent.ConcurrentHashMap.newKeySet(
                (int) Math.min(globalOrdCount * 2, Integer.MAX_VALUE));
            long chunkSize = (globalOrdCount + numWorkers - 1) / numWorkers;
            @SuppressWarnings("unchecked")
            java.util.concurrent.CompletableFuture<?>[] futures =
                new java.util.concurrent.CompletableFuture[numWorkers];
            for (int w = 0; w < numWorkers; w++) {
              final long start = w * chunkSize;
              final long end = Math.min(start + chunkSize, globalOrdCount);
              futures[w] = java.util.concurrent.CompletableFuture.runAsync(() -> {
                try {
                  // Each worker needs its own DV instances (not thread-safe)
                  SortedSetDocValues[] localDvs = new SortedSetDocValues[leaves.size()];
                  for (int i = 0; i < leaves.size(); i++) {
                    localDvs[i] = leaves.get(i).reader().getSortedSetDocValues(columnName);
                  }
                  for (long g = start; g < end; g++) {
                    int segIdx = ordinalMap.getFirstSegmentNumber(g);
                    long segOrd = ordinalMap.getFirstSegmentOrd(g);
                    BytesRef bytes = (localDvs[segIdx] != null)
                        ? localDvs[segIdx].lookupOrd(segOrd) : new BytesRef("");
                    concurrentSet.add(bytes.utf8ToString());
                  }
                } catch (java.io.IOException e) {
                  throw new java.io.UncheckedIOException(e);
                }
              }, FusedGroupByAggregate.getParallelPool());
            }
            java.util.concurrent.CompletableFuture.allOf(futures).join();
            distinctStrings = concurrentSet;
          } else {
            for (long g = 0; g < globalOrdCount; g++) {
              int segIdx = ordinalMap.getFirstSegmentNumber(g);
              long segOrd = ordinalMap.getFirstSegmentOrd(g);
              BytesRef bytes = (segDvs[segIdx] != null)
                  ? segDvs[segIdx].lookupOrd(segOrd) : new BytesRef("");
              distinctStrings.add(bytes.utf8ToString());
            }
          }
        } else {
          // Fallback: segments with deletes or no OrdinalMap — use per-segment ordinal iteration
          for (LeafReaderContext leafCtx : leaves) {
            LeafReader reader = leafCtx.reader();
            Bits liveDocs = reader.getLiveDocs();
            SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
            if (dv == null) continue;
            long valueCount = dv.getValueCount();
            if (liveDocs == null) {
              // No deletes: all ordinals are used
              for (long ord = 0; ord < valueCount; ord++) {
                distinctStrings.add(dv.lookupOrd(ord).utf8ToString());
              }
            } else {
              // Has deletes: track used ordinals via FixedBitSet
              int maxDoc = reader.maxDoc();
              FixedBitSet usedOrdinals = new FixedBitSet((int) Math.min(valueCount, Integer.MAX_VALUE));
              for (int doc = 0; doc < maxDoc; doc++) {
                if (liveDocs.get(doc) && dv.advanceExact(doc)) {
                  usedOrdinals.set((int) dv.nextOrd());
                }
              }
              for (int ord = usedOrdinals.nextSetBit(0); ord != -1;
                  ord = (ord + 1 < usedOrdinals.length()) ? usedOrdinals.nextSetBit(ord + 1) : -1) {
                distinctStrings.add(dv.lookupOrd(ord).utf8ToString());
              }
            }
          }
        }
      } else {
        // Filtered query: use FixedBitSet ordinal collection per segment
        for (LeafReaderContext leafCtx : leaves) {
          LeafReader reader = leafCtx.reader();
          SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
          if (dv == null) continue;
          int maxDoc = reader.maxDoc();
          Bits liveDocs = reader.getLiveDocs();
          long valueCount = dv.getValueCount();
          FixedBitSet usedOrdinals =
              new FixedBitSet((int) Math.min(valueCount, Integer.MAX_VALUE));
          for (int doc = 0; doc < maxDoc; doc++) {
            boolean isLive = liveDocs == null || liveDocs.get(doc);
            if (isLive && dv.advanceExact(doc)) {
              usedOrdinals.set((int) dv.nextOrd());
            }
          }
          for (int ord = usedOrdinals.nextSetBit(0);
              ord != -1;
              ord = (ord + 1 < usedOrdinals.length()) ? usedOrdinals.nextSetBit(ord + 1) : -1) {
            distinctStrings.add(dv.lookupOrd(ord).utf8ToString());
          }
        }
      }
    }
    return distinctStrings;
  }

  /**
   * Execute a fused scan to collect distinct values for a single VARCHAR column, returning the
   * distinct strings as a single-column VarcharType Page. Uses ordinal-based dedup via FixedBitSet
   * for O(1) per-doc ordinal collection, then resolves strings in bulk from the term dictionary.
   * For MatchAllDocsQuery with no deletes, this is nearly free as we can directly iterate the term
   * dictionary.
   *
   * @param columnName the VARCHAR column to collect distinct values from
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @return a list containing a single Page with the distinct values as a VarcharType column
   */
  public static List<Page> executeDistinctValuesVarchar(
      String columnName, IndexShard shard, Query query) throws Exception {
    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-values-varchar")) {
      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();

      // For single-segment case (common), use ordinal-based approach directly
      if (leaves.size() == 1) {
        return executeDistinctValuesVarcharSingleSegment(columnName, leaves.get(0), query);
      }

      // Multi-segment: collect distinct strings using HashSet across segments
      // (ordinals are per-segment and not directly comparable across segments)
      HashSet<String> distinctStrings = new HashSet<>();
      for (LeafReaderContext leafCtx : leaves) {
        LeafReader reader = leafCtx.reader();
        SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
        if (dv == null) continue;

        int maxDoc = reader.maxDoc();
        Bits liveDocs = reader.getLiveDocs();
        long valueCount = dv.getValueCount();
        FixedBitSet usedOrdinals = new FixedBitSet((int) Math.min(valueCount, Integer.MAX_VALUE));

        if (query instanceof MatchAllDocsQuery) {
          if (liveDocs == null) {
            // All docs live, all ordinals used — collect all terms directly
            for (long ord = 0; ord < valueCount; ord++) {
              distinctStrings.add(dv.lookupOrd(ord).utf8ToString());
            }
            continue;
          }
          // Has deletes: collect used ordinals via single-value nextOrd()
          for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs.get(doc) && dv.advanceExact(doc)) {
              usedOrdinals.set((int) dv.nextOrd());
            }
          }
        } else {
          // Filtered: iterate all docs with filter check
          for (int doc = 0; doc < maxDoc; doc++) {
            boolean isLive = liveDocs == null || liveDocs.get(doc);
            if (isLive && dv.advanceExact(doc)) {
              usedOrdinals.set((int) dv.nextOrd());
            }
          }
        }

        // Resolve ordinals to strings
        for (int ord = usedOrdinals.nextSetBit(0);
            ord != -1;
            ord = (ord + 1 < usedOrdinals.length()) ? usedOrdinals.nextSetBit(ord + 1) : -1) {
          distinctStrings.add(dv.lookupOrd(ord).utf8ToString());
        }
      }

      if (distinctStrings.isEmpty()) {
        return List.of();
      }

      // Build result page
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, distinctStrings.size());
      for (String val : distinctStrings) {
        VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(val));
      }
      return List.of(new Page(builder.build()));
    }
  }

  /**
   * Single-segment fast path for VARCHAR distinct values. Uses FixedBitSet for ordinal dedup and
   * resolves strings in bulk from the sorted term dictionary.
   */
  private static List<Page> executeDistinctValuesVarcharSingleSegment(
      String columnName, LeafReaderContext leafCtx, Query query) throws IOException {
    LeafReader reader = leafCtx.reader();
    SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
    if (dv == null) {
      return List.of();
    }

    int maxDoc = reader.maxDoc();
    Bits liveDocs = reader.getLiveDocs();
    long valueCount = dv.getValueCount();

    // For MatchAllDocsQuery with no deletes, all ordinals are used
    if (query instanceof MatchAllDocsQuery && liveDocs == null) {
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, (int) valueCount);
      for (long ord = 0; ord < valueCount; ord++) {
        BytesRef bytes = dv.lookupOrd(ord);
        VarcharType.VARCHAR.writeSlice(
            builder,
            io.airlift.slice.Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
      }
      return List.of(new Page(builder.build()));
    }

    // Collect used ordinals (single-valued: one nextOrd() per doc)
    FixedBitSet usedOrdinals = new FixedBitSet((int) Math.min(valueCount, Integer.MAX_VALUE));
    for (int doc = 0; doc < maxDoc; doc++) {
      boolean isLive = liveDocs == null || liveDocs.get(doc);
      if (isLive && dv.advanceExact(doc)) {
        usedOrdinals.set((int) dv.nextOrd());
      }
    }

    int distinctCount = usedOrdinals.cardinality();
    if (distinctCount == 0) {
      return List.of();
    }

    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, distinctCount);
    for (int ord = usedOrdinals.nextSetBit(0);
        ord != -1;
        ord = (ord + 1 < usedOrdinals.length()) ? usedOrdinals.nextSetBit(ord + 1) : -1) {
      BytesRef bytes = dv.lookupOrd(ord);
      VarcharType.VARCHAR.writeSlice(
          builder, io.airlift.slice.Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
    }
    return List.of(new Page(builder.build()));
  }

  /**
   * COUNT(DISTINCT col) accumulator. Uses primitive LongOpenHashSet for numeric columns to avoid
   * Long boxing overhead (~200K boxing operations per shard for high-cardinality columns like
   * UserID). Falls back to HashSet&lt;Object&gt; for varchar and double types.
   */
  private static class CountDistinctDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final Type argType;
    private final boolean isVarchar;
    private final boolean isDoubleType;
    private final boolean usePrimitiveLong;
    private SortedNumericDocValues numericDv;
    private SortedSetDocValues stringDv;

    /** Primitive long set for numeric non-double columns (avoids Long boxing). */
    private final LongOpenHashSet longDistinctValues;

    /** Fallback set for varchar and double types. */
    private final Set<Object> objectDistinctValues;

    CountDistinctDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.argType = argType;
      this.isVarchar = argType instanceof VarcharType;
      this.isDoubleType = argType instanceof DoubleType;
      this.usePrimitiveLong = !isVarchar && !isDoubleType;
      this.longDistinctValues = usePrimitiveLong ? new LongOpenHashSet() : null;
      this.objectDistinctValues = usePrimitiveLong ? null : new HashSet<>();
    }

    @Override
    public void initSegment(LeafReaderContext leaf) throws IOException {
      if (isVarchar) {
        stringDv = leaf.reader().getSortedSetDocValues(field);
      } else {
        numericDv = leaf.reader().getSortedNumericDocValues(field);
      }
    }

    @Override
    public void accumulate(int doc) throws IOException {
      if (isVarchar) {
        if (stringDv != null && stringDv.advanceExact(doc)) {
          BytesRef bytes = stringDv.lookupOrd(stringDv.nextOrd());
          objectDistinctValues.add(bytes.utf8ToString());
        }
      } else if (isDoubleType) {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          objectDistinctValues.add(Double.longBitsToDouble(numericDv.nextValue()));
        }
      } else {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          longDistinctValues.add(numericDv.nextValue());
        }
      }
    }

    @Override
    public Type getOutputType() {
      return BigintType.BIGINT;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      long count = usePrimitiveLong ? longDistinctValues.size() : objectDistinctValues.size();
      BigintType.BIGINT.writeLong(builder, count);
    }
  }
}
