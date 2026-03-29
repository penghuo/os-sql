# COUNT(DISTINCT) Coordinator Merge Methods
## File: dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java

---

## Method 1: mergeCountDistinctValuesViaRawSets (Line 1985) — Q04 Global Numeric COUNT(DISTINCT)

1985:   private static List<Page> mergeCountDistinctValuesViaRawSets(
1986:       ShardExecuteResponse[] shardResults, List<List<Page>> shardPages) {
1987:     // Check if all shards have raw sets
1988:     boolean allHaveRawSets = true;
1989:     for (ShardExecuteResponse sr : shardResults) {
1990:       if (sr.getScalarDistinctSet() == null) {
1991:         allHaveRawSets = false;
1992:         break;
1993:       }
1994:     }
1995:     if (!allHaveRawSets) {
1996:       return mergeCountDistinctValues(shardPages);
1997:     }
1998: 
1999:     // Fast path: union raw LongOpenHashSets directly.
2000:     // Pick the largest shard set as the base and merge others into it.
2001:     org.opensearch.sql.dqe.operator.LongOpenHashSet largest = null;
2002:     int largestSize = -1;
2003:     int largestIdx = -1;
2004:     for (int i = 0; i < shardResults.length; i++) {
2005:       org.opensearch.sql.dqe.operator.LongOpenHashSet set = shardResults[i].getScalarDistinctSet();
2006:       if (set.size() > largestSize) {
2007:         largest = set;
2008:         largestSize = set.size();
2009:         largestIdx = i;
2010:       }
2011:     }
2012:     // Merge all other sets into the largest
2013:     for (int i = 0; i < shardResults.length; i++) {
2014:       if (i == largestIdx) continue;
2015:       org.opensearch.sql.dqe.operator.LongOpenHashSet other =
2016:           shardResults[i].getScalarDistinctSet();
2017:       // Iterate the raw keys array to add all entries
2018:       if (other.hasZeroValue()) {
2019:         largest.add(0L);
2020:       }
2021:       if (other.hasSentinelValue()) {
2022:         largest.add(org.opensearch.sql.dqe.operator.LongOpenHashSet.emptyMarker());
2023:       }
2024:       long[] otherKeys = other.keys();
2025:       long emptyMarker = org.opensearch.sql.dqe.operator.LongOpenHashSet.emptyMarker();
2026:       for (int j = 0; j < otherKeys.length; j++) {
2027:         if (otherKeys[j] != emptyMarker) {
2028:           largest.add(otherKeys[j]);
2029:         }
2030:       }
2031:     }
2032:     io.trino.spi.block.BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
2033:     BigintType.BIGINT.writeLong(builder, largest.size());
2034:     return List.of(new Page(builder.build()));
2035:   }
2036: 
2037:   /**
2038:    * Merge scalar COUNT(DISTINCT varcharCol) using raw string set attachments from shards. If all
2039:    * shards have the raw set, unions them directly (no Page extraction). Falls back to Page-based
2040:    * merge if any shard lacks the attachment.

---

## Method 2: mergeCountDistinctVarcharViaRawSets (Line 2042) — Q05 Global VARCHAR COUNT(DISTINCT)

2042:   private static List<Page> mergeCountDistinctVarcharViaRawSets(
2043:       ShardExecuteResponse[] shardResults, List<List<Page>> shardPages) {
2044:     // Check if all shards have raw string sets
2045:     boolean allHaveRawSets = true;
2046:     for (ShardExecuteResponse sr : shardResults) {
2047:       if (sr.getScalarDistinctStrings() == null) {
2048:         allHaveRawSets = false;
2049:         break;
2050:       }
2051:     }
2052:     if (!allHaveRawSets) {
2053:       return mergeCountDistinctVarcharValues(shardPages);
2054:     }
2055: 
2056:     // Fast path: union raw HashSet<String> directly.
2057:     // Pick the largest shard set as the base and merge others into it.
2058:     java.util.Set<String> largest = null;
2059:     int largestSize = -1;
2060:     int largestIdx = -1;
2061:     for (int i = 0; i < shardResults.length; i++) {
2062:       java.util.Set<String> set = shardResults[i].getScalarDistinctStrings();
2063:       if (set.size() > largestSize) {
2064:         largest = set;
2065:         largestSize = set.size();
2066:         largestIdx = i;
2067:       }
2068:     }
2069:     for (int i = 0; i < shardResults.length; i++) {
2070:       if (i == largestIdx) continue;
2071:       largest.addAll(shardResults[i].getScalarDistinctStrings());
2072:     }
2073:     io.trino.spi.block.BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
2074:     BigintType.BIGINT.writeLong(builder, largest.size());
2075:     return List.of(new Page(builder.build()));
2076:   }
2077: 
2078:   /**
2079:    * Check if the shard plan uses dedup for COUNT(DISTINCT) queries. Returns true when the shard
2080:    * plan is a PARTIAL AggregationNode (created by PlanFragmenter for dedup), the coordinator plan
2081:    * is a SINGLE AggregationNode with GROUP BY + COUNT(DISTINCT), and the shard aggregates exactly
2082:    * COUNT(*). This enables the two-stage merge (FINAL dedup + row counting) which avoids the
2083:    * expensive HashSet-based COUNT(DISTINCT) accumulators in runCoordinatorAggregation.
2084:    */
2085:   private static boolean isShardDedupCountDistinct(
2086:       DqePlanNode shardPlan, AggregationNode singleAgg, Map<String, Type> columnTypeMap) {
2087:     if (!(shardPlan instanceof AggregationNode shardAgg)) {
2088:       return false;
2089:     }
2090:     if (shardAgg.getStep() != AggregationNode.Step.PARTIAL) {
2091:       return false;
2092:     }
2093:     if (singleAgg.getGroupByKeys().isEmpty()) {
2094:       return false;
2095:     }
2096:     // Shard dedup has more group-by keys than the original (original keys + distinct columns)
2097:     if (shardAgg.getGroupByKeys().size() <= singleAgg.getGroupByKeys().size()
2098:         || shardAgg.getAggregateFunctions().size() != 1
2099:         || !"COUNT(*)".equals(shardAgg.getAggregateFunctions().get(0))) {
2100:       return false;
2101:     }
2102:     return true;
2103:   }
2104: 
2105:   /**
2106:    * Two-stage merge for shard-deduped COUNT(DISTINCT) queries. Shards produce pre-deduped (key +
2107:    * distinct_col, COUNT(*)) tuples. The coordinator:
2108:    *

---

## Method 3: mergeDedupCountDistinct (Line 2117) — Generic Two-Stage Merge (GROUP BY + COUNT DISTINCT)

2109:    * <ol>
2110:    *   <li>Stage 1: FINAL merge for dedup keys to remove cross-shard duplicates
2111:    *   <li>Stage 2: GROUP BY original keys with COUNT(*) to get COUNT(DISTINCT)
2112:    * </ol>
2113:    *
2114:    * This leverages the fast FINAL merge path (O(n) hash merge) instead of the generic
2115:    * HashAggregationOperator with per-row CountDistinctAccumulator (HashSet per group).
2116:    */
2117:   private static List<Page> mergeDedupCountDistinct(
2118:       List<List<Page>> shardPages,
2119:       AggregationNode singleAgg,
2120:       DqePlanNode shardPlan,
2121:       List<Type> columnTypes,
2122:       Map<String, Type> columnTypeMap,
2123:       ResultMerger merger) {
2124:     AggregationNode shardAgg = (AggregationNode) shardPlan;
2125:     List<String> dedupKeys = shardAgg.getGroupByKeys();
2126:     List<String> originalKeys = singleAgg.getGroupByKeys();
2127: 
2128:     // Stage 1: FINAL merge for dedup keys (removes cross-shard duplicates)
2129:     // Build types for the dedup output: [dedupKey types..., BigintType for COUNT(*)]
2130:     List<Type> dedupTypes = new ArrayList<>();
2131:     for (String key : dedupKeys) {
2132:       dedupTypes.add(columnTypeMap.getOrDefault(key, BigintType.BIGINT));
2133:     }
2134:     dedupTypes.add(BigintType.BIGINT); // COUNT(*) column
2135: 
2136:     int numOriginalKeys = originalKeys.size();
2137:     int numDedupKeys = dedupKeys.size();
2138:     int numCountDistinctAggs = singleAgg.getAggregateFunctions().size();
2139: 
2140:     // === Fused path: all-numeric dedup keys with single original numeric key ===
2141:     // Combines Stage 1 (FINAL dedup merge) and Stage 2 (row counting by original keys)
2142:     // into a single pass, eliminating intermediate Page construction.
2143:     boolean allNumericDedup = true;
2144:     for (int i = 0; i < numDedupKeys; i++) {
2145:       Type t = dedupTypes.get(i);
2146:       if (!(t instanceof BigintType
2147:           || t instanceof IntegerType
2148:           || t instanceof io.trino.spi.type.TimestampType
2149:           || t instanceof io.trino.spi.type.SmallintType
2150:           || t instanceof io.trino.spi.type.TinyintType
2151:           || t instanceof io.trino.spi.type.BooleanType)) {
2152:         allNumericDedup = false;
2153:         break;
2154:       }
2155:     }
2156: 
2157:     if (allNumericDedup && numOriginalKeys == 1 && numDedupKeys == 2) {
2158:       // Ultra-fast 2-key fused path: uses two flat long[] arrays instead of long[][]
2159:       // to eliminate all per-entry array allocation and indirection. For Q9-style
2160:       // GROUP BY RegionID COUNT(DISTINCT UserID), this avoids 80K long[2] allocations.
2161:       return mergeDedupCountDistinct2Key(
2162:           shardPages, dedupTypes, numOriginalKeys, numCountDistinctAggs);
2163:     }
2164: 
2165:     if (allNumericDedup && numOriginalKeys == 1) {
2166:       // Fused: Build dedup hashmap from shard pages, then count per original key.
2167:       // Uses open-addressing hash map with long[] keys for zero-allocation inner loop.
2168:       int capacity = 1024;
2169:       float loadFactor = 0.7f;
2170:       int threshold = (int) (capacity * loadFactor);
2171:       int size = 0;
2172:       long[][] mapKeys = new long[capacity][];
2173:       long[] mapCounts = new long[capacity];
2174:       boolean[] mapOccupied = new boolean[capacity];
2175:       long[] tmpKey = new long[numDedupKeys];
2176: 
2177:       for (java.util.List<Page> shardPageList : shardPages) {
2178:         for (Page page : shardPageList) {
2179:           int positionCount = page.getPositionCount();
2180:           Block[] keyBlocks = new Block[numDedupKeys];
2181:           for (int k = 0; k < numDedupKeys; k++) {
2182:             keyBlocks[k] = page.getBlock(k);
2183:           }
2184:           Block countBlock = page.getBlock(numDedupKeys); // COUNT(*) column
2185: 
2186:           for (int pos = 0; pos < positionCount; pos++) {
2187:             for (int k = 0; k < numDedupKeys; k++) {
2188:               tmpKey[k] = dedupTypes.get(k).getLong(keyBlocks[k], pos);
2189:             }
2190:             long cnt = BigintType.BIGINT.getLong(countBlock, pos);
2191:             int hash = 1;
2192:             for (int k = 0; k < numDedupKeys; k++) {
2193:               hash = hash * 31 + Long.hashCode(tmpKey[k]);
2194:             }
2195:             int mask = capacity - 1;
2196:             int slot = hash & mask;
2197:             while (true) {
2198:               if (!mapOccupied[slot]) {
2199:                 mapKeys[slot] = tmpKey.clone();
2200:                 mapCounts[slot] = cnt;
2201:                 mapOccupied[slot] = true;
2202:                 size++;
2203:                 if (size > threshold) {
2204:                   int newCap = capacity * 2;
2205:                   long[][] nk = new long[newCap][];
2206:                   long[] nc = new long[newCap];
2207:                   boolean[] no = new boolean[newCap];
2208:                   int nm = newCap - 1;
2209:                   for (int s = 0; s < capacity; s++) {
2210:                     if (mapOccupied[s]) {
2211:                       int h = 1;
2212:                       for (int k = 0; k < numDedupKeys; k++) {
2213:                         h = h * 31 + Long.hashCode(mapKeys[s][k]);
2214:                       }
2215:                       int ns = h & nm;
2216:                       while (no[ns]) ns = (ns + 1) & nm;
2217:                       nk[ns] = mapKeys[s];
2218:                       nc[ns] = mapCounts[s];
2219:                       no[ns] = true;
2220:                     }
2221:                   }
2222:                   mapKeys = nk;
2223:                   mapCounts = nc;
2224:                   mapOccupied = no;
2225:                   capacity = newCap;
2226:                   mask = nm;
2227:                   threshold = (int) (newCap * loadFactor);
2228:                   slot = hash & mask;
2229:                   while (mapOccupied[slot]) {
2230:                     long[] existing = mapKeys[slot];
2231:                     boolean match = true;
2232:                     for (int k = 0; k < numDedupKeys; k++) {
2233:                       if (existing[k] != tmpKey[k]) {
2234:                         match = false;
2235:                         break;
2236:                       }
2237:                     }
2238:                     if (match) break;
2239:                     slot = (slot + 1) & mask;
2240:                   }
2241:                 }
2242:                 break;
2243:               }
2244:               long[] existing = mapKeys[slot];
2245:               boolean match = true;
2246:               for (int k = 0; k < numDedupKeys; k++) {
2247:                 if (existing[k] != tmpKey[k]) {
2248:                   match = false;
2249:                   break;
2250:                 }
2251:               }
2252:               if (match) {
2253:                 mapCounts[slot] += cnt; // SUM the partial counts
2254:                 break;
2255:               }
2256:               slot = (slot + 1) & mask;
2257:             }
2258:           }
2259:         }
2260:       }
2261: 
2262:       if (size == 0) {
2263:         return List.of();
2264:       }
2265: 
2266:       // Stage 2: Count entries per original key (first dedup key)
2267:       java.util.HashMap<Long, Long> groupCounts = new java.util.HashMap<>();
2268:       for (int s = 0; s < capacity; s++) {
2269:         if (!mapOccupied[s]) continue;
2270:         long origKey = mapKeys[s][0];
2271:         groupCounts.merge(origKey, 1L, Long::sum);
2272:       }
2273: 
2274:       // Build result page
2275:       Type keyType = dedupTypes.get(0);
2276:       int numOutputCols = numOriginalKeys + numCountDistinctAggs;
2277:       io.trino.spi.block.BlockBuilder[] builders =
2278:           new io.trino.spi.block.BlockBuilder[numOutputCols];
2279:       builders[0] = keyType.createBlockBuilder(null, groupCounts.size());
2280:       for (int i = numOriginalKeys; i < numOutputCols; i++) {
2281:         builders[i] = BigintType.BIGINT.createBlockBuilder(null, groupCounts.size());
2282:       }
2283: 
2284:       for (var entry : groupCounts.entrySet()) {
2285:         long key = entry.getKey();
2286:         if (keyType instanceof IntegerType) {
2287:           IntegerType.INTEGER.writeLong(builders[0], (int) key);
2288:         } else {
2289:           keyType.writeLong(builders[0], key);
2290:         }
2291:         BigintType.BIGINT.writeLong(builders[numOriginalKeys], entry.getValue());
2292:       }
2293: 
2294:       Block[] blocks = new Block[numOutputCols];
2295:       for (int i = 0; i < numOutputCols; i++) {
2296:         blocks[i] = builders[i].build();
2297:       }
2298:       return List.of(new Page(blocks));
2299:     }
2300: 
2301:     // === Fused path: VARCHAR original key + numeric distinct column ===
2302:     // For Q14-style queries: GROUP BY SearchPhrase COUNT(DISTINCT UserID)
2303:     // Dedup keys are [SearchPhrase (VARCHAR), UserID (BIGINT)], original key is [SearchPhrase].
2304:     // Uses SliceLongDedupMap for zero-copy dedup, then SliceCountMap for group counting.
2305:     if (!allNumericDedup
2306:         && numOriginalKeys == 1
2307:         && numDedupKeys == 2
2308:         && dedupTypes.get(0) instanceof VarcharType
2309:         && isNumericType(dedupTypes.get(1))) {
2310:       return mergeDedupCountDistinctVarcharKey(shardPages, dedupTypes, numCountDistinctAggs);
2311:     }
2312: 
2313:     // === Fallback: two-stage merge via intermediate Pages ===
2314:     AggregationNode finalDedupNode =
2315:         new AggregationNode(null, dedupKeys, List.of("COUNT(*)"), AggregationNode.Step.FINAL);
2316:     List<Page> dedupedPages = merger.mergeAggregation(shardPages, finalDedupNode, dedupTypes);
2317: 
2318:     Type[] keyTypes = new Type[numOriginalKeys];
2319:     for (int i = 0; i < numOriginalKeys; i++) {
2320:       keyTypes[i] = dedupTypes.get(i);
2321:     }
2322: 
2323:     java.util.LinkedHashMap<Object, Long> groupCounts = new java.util.LinkedHashMap<>();
2324: 
2325:     for (Page page : dedupedPages) {
2326:       if (numOriginalKeys == 1) {
2327:         Block keyBlock = page.getBlock(0);
2328:         Type keyType = keyTypes[0];
2329:         for (int pos = 0; pos < page.getPositionCount(); pos++) {
2330:           Object key = extractValue(page, 0, pos, keyType);
2331:           groupCounts.merge(key, 1L, Long::sum);
2332:         }
2333:       } else {
2334:         for (int pos = 0; pos < page.getPositionCount(); pos++) {
2335:           List<Object> key = new ArrayList<>(numOriginalKeys);
2336:           for (int i = 0; i < numOriginalKeys; i++) {
2337:             key.add(extractValue(page, i, pos, keyTypes[i]));
2338:           }
2339:           groupCounts.merge(key, 1L, Long::sum);
2340:         }
2341:       }
2342:     }
2343: 
2344:     if (groupCounts.isEmpty()) {
2345:       return List.of();
2346:     }
2347: 
2348:     int numOutputCols = numOriginalKeys + numCountDistinctAggs;
2349:     io.trino.spi.block.BlockBuilder[] builders = new io.trino.spi.block.BlockBuilder[numOutputCols];
2350:     for (int i = 0; i < numOriginalKeys; i++) {
2351:       builders[i] = keyTypes[i].createBlockBuilder(null, groupCounts.size());
2352:     }
2353:     for (int i = numOriginalKeys; i < numOutputCols; i++) {
2354:       builders[i] = BigintType.BIGINT.createBlockBuilder(null, groupCounts.size());
2355:     }
2356: 
2357:     for (var entry : groupCounts.entrySet()) {
2358:       Object key = entry.getKey();
2359:       if (numOriginalKeys == 1) {
2360:         appendTypedValue(builders[0], keyTypes[0], key);
2361:       } else {
2362:         @SuppressWarnings("unchecked")
2363:         List<Object> multiKey = (List<Object>) key;
2364:         for (int i = 0; i < numOriginalKeys; i++) {
2365:           appendTypedValue(builders[i], keyTypes[i], multiKey.get(i));
2366:         }
2367:       }
2368:       BigintType.BIGINT.writeLong(builders[numOriginalKeys], entry.getValue());
2369:     }
2370: 
2371:     Block[] blocks = new Block[numOutputCols];
2372:     for (int i = 0; i < numOutputCols; i++) {
2373:       blocks[i] = builders[i].build();
2374:     }
2375:     return List.of(new Page(blocks));
2376:   }
2377: 
2378:   /**
2379:    * Ultra-fast 2-key fused merge for COUNT(DISTINCT) with exactly 2 numeric dedup keys and 1
2380:    * numeric original key. Uses two flat long[] arrays instead of long[][] to eliminate all
2381:    * per-entry array allocation. For Q9-style queries (GROUP BY RegionID, COUNT(DISTINCT UserID)),
2382:    * this processes ~80K shard rows with zero allocation in the inner loop.
2383:    *
2384:    * <p>Stage 1: Build open-addressing hash map from (key0, key1) pairs (set insertion, ignoring
2385:    * counts). Stage 2: Count entries per original key (key0) to get COUNT(DISTINCT).
2386:    */
2387:   /**
2388:    * Fast COUNT(DISTINCT) merge using pre-built per-group HashSets from shard responses. When topN >
2389:    * 0, uses lazy merge with upper/lower-bound pruning to only merge candidate groups (typically
2390:    * ~10% of all groups), reducing coordinator merge time by ~8x for ORDER BY LIMIT queries.
2391:    */

---

## Method 4: mergeDedupCountDistinctViaSets (Line 2392) — Numeric HashSet Merge (GROUP BY + COUNT DISTINCT via Sets)

2384:    * <p>Stage 1: Build open-addressing hash map from (key0, key1) pairs (set insertion, ignoring
2385:    * counts). Stage 2: Count entries per original key (key0) to get COUNT(DISTINCT).
2386:    */
2387:   /**
2388:    * Fast COUNT(DISTINCT) merge using pre-built per-group HashSets from shard responses. When topN >
2389:    * 0, uses lazy merge with upper/lower-bound pruning to only merge candidate groups (typically
2390:    * ~10% of all groups), reducing coordinator merge time by ~8x for ORDER BY LIMIT queries.
2391:    */
2392:   private static List<Page> mergeDedupCountDistinctViaSets(
2393:       ShardExecuteResponse[] shardResults,
2394:       AggregationNode singleAgg,
2395:       List<Type> columnTypes,
2396:       Map<String, Type> columnTypeMap,
2397:       long topN) {
2398: 
2399:     String groupKey = singleAgg.getGroupByKeys().get(0);
2400:     Type keyType = columnTypeMap.getOrDefault(groupKey, BigintType.BIGINT);
2401:     int numCountDistinctAggs = singleAgg.getAggregateFunctions().size();
2402:     int numShards = shardResults.length;
2403: 
2404:     // Collect all per-group per-shard sets indexed by group key
2405:     java.util.HashMap<Long, java.util.List<org.opensearch.sql.dqe.operator.LongOpenHashSet>>
2406:         groupShardSets = new java.util.HashMap<>();
2407:     for (ShardExecuteResponse sr : shardResults) {
2408:       java.util.Map<Long, org.opensearch.sql.dqe.operator.LongOpenHashSet> shardSets =
2409:           sr.getDistinctSets();
2410:       if (shardSets == null) continue;
2411:       for (var entry : shardSets.entrySet()) {
2412:         groupShardSets
2413:             .computeIfAbsent(entry.getKey(), k -> new java.util.ArrayList<>(numShards))
2414:             .add(entry.getValue());
2415:       }
2416:     }
2417: 
2418:     if (groupShardSets.isEmpty()) return List.of();
2419: 
2420:     // Determine which groups to merge (all groups, or top-K candidates if topN is set)
2421:     java.util.Set<Long> candidateKeys;
2422:     int totalGroups = groupShardSets.size();
2423: 
2424:     if (topN > 0 && topN * 10 < totalGroups) {
2425:       // Lazy merge: compute upper bounds (sum of shard sizes) and prune
2426:       int candidateCount = (int) Math.min(topN * 10, totalGroups);
2427:       long[] groupKeyArr = new long[totalGroups];
2428:       long[] upperBounds = new long[totalGroups];
2429:       int idx = 0;
2430:       for (var entry : groupShardSets.entrySet()) {
2431:         groupKeyArr[idx] = entry.getKey();
2432:         long upper = 0;
2433:         for (var set : entry.getValue()) upper += set.size();
2434:         upperBounds[idx] = upper;
2435:         idx++;
2436:       }
2437:       // Partial sort: find top candidateCount by upper bound using min-heap
2438:       int[] heap = new int[candidateCount];
2439:       int heapSize = 0;
2440:       for (int i = 0; i < totalGroups; i++) {
2441:         if (heapSize < candidateCount) {
2442:           heap[heapSize] = i;
2443:           heapSize++;
2444:           // Sift up
2445:           int k = heapSize - 1;
2446:           while (k > 0) {
2447:             int parent = (k - 1) >>> 1;
2448:             if (upperBounds[heap[k]] < upperBounds[heap[parent]]) {
2449:               int tmp = heap[k];
2450:               heap[k] = heap[parent];
2451:               heap[parent] = tmp;
2452:               k = parent;
2453:             } else break;
2454:           }
2455:         } else if (upperBounds[i] > upperBounds[heap[0]]) {
2456:           heap[0] = i;
2457:           // Sift down
2458:           int k = 0;
2459:           while (true) {
2460:             int left = 2 * k + 1;
2461:             if (left >= heapSize) break;
2462:             int right = left + 1;
2463:             int target = left;
2464:             if (right < heapSize && upperBounds[heap[right]] < upperBounds[heap[left]])
2465:               target = right;
2466:             if (upperBounds[heap[target]] < upperBounds[heap[k]]) {
2467:               int tmp = heap[k];
2468:               heap[k] = heap[target];
2469:               heap[target] = tmp;
2470:               k = target;
2471:             } else break;
2472:           }
2473:         }
2474:       }
2475:       candidateKeys = new java.util.HashSet<>(heapSize * 2);
2476:       for (int h = 0; h < heapSize; h++) candidateKeys.add(groupKeyArr[heap[h]]);
2477:     } else {
2478:       candidateKeys = groupShardSets.keySet(); // merge all
2479:     }
2480: 
2481:     // Merge only candidate groups — take ownership of first shard's set (zero-copy)
2482:     java.util.HashMap<Long, org.opensearch.sql.dqe.operator.LongOpenHashSet> merged =
2483:         new java.util.HashMap<>(candidateKeys.size() * 2);
2484:     long emptyMarker = org.opensearch.sql.dqe.operator.LongOpenHashSet.emptyMarker();
2485: 
2486:     for (Long gk : candidateKeys) {
2487:       java.util.List<org.opensearch.sql.dqe.operator.LongOpenHashSet> sets = groupShardSets.get(gk);
2488:       if (sets == null || sets.isEmpty()) continue;
2489: 
2490:       // Take ownership of the largest shard's set as the merge target (avoid copying its entries)
2491:       org.opensearch.sql.dqe.operator.LongOpenHashSet target = sets.get(0);
2492:       int largestIdx = 0;
2493:       int largestSize = target.size();
2494:       for (int s = 1; s < sets.size(); s++) {
2495:         if (sets.get(s).size() > largestSize) {
2496:           largestSize = sets.get(s).size();
2497:           largestIdx = s;
2498:           target = sets.get(s);
2499:         }
2500:       }
2501: 
2502:       // Merge remaining shards into target
2503:       for (int s = 0; s < sets.size(); s++) {
2504:         if (s == largestIdx) continue;
2505:         org.opensearch.sql.dqe.operator.LongOpenHashSet source = sets.get(s);
2506:         if (source.hasZeroValue()) target.add(0);
2507:         if (source.hasSentinelValue()) target.add(emptyMarker);
2508:         long[] sourceTable = source.keys();
2509:         for (int i = 0; i < sourceTable.length; i++) {
2510:           if (sourceTable[i] != emptyMarker) {
2511:             target.add(sourceTable[i]);
2512:           }
2513:         }
2514:       }
2515:       merged.put(gk, target);
2516:     }
2517: 
2518:     if (merged.isEmpty()) return List.of();
2519: 
2520:     // Build result page
2521:     int numOutputCols = 1 + numCountDistinctAggs;
2522:     io.trino.spi.block.BlockBuilder[] builders = new io.trino.spi.block.BlockBuilder[numOutputCols];
2523:     builders[0] = keyType.createBlockBuilder(null, merged.size());
2524:     for (int i = 1; i < numOutputCols; i++) {
2525:       builders[i] = BigintType.BIGINT.createBlockBuilder(null, merged.size());
2526:     }
2527: 
2528:     for (var entry : merged.entrySet()) {
2529:       long key = entry.getKey();
2530:       long count = entry.getValue().size();
2531:       if (keyType instanceof IntegerType) {
2532:         IntegerType.INTEGER.writeLong(builders[0], (int) key);
2533:       } else {
2534:         keyType.writeLong(builders[0], key);
2535:       }
2536:       for (int i = 1; i < numOutputCols; i++) {
2537:         BigintType.BIGINT.writeLong(builders[i], count);
2538:       }
2539:     }
2540: 
2541:     Block[] blocks = new Block[numOutputCols];
2542:     for (int i = 0; i < numOutputCols; i++) {
2543:       blocks[i] = builders[i].build();
2544:     }
2545:     return List.of(new Page(blocks));
2546:   }
2547: 
2548:   /**
2549:    * Merge VARCHAR-keyed COUNT(DISTINCT) results using attached per-group LongOpenHashSets. Each
2550:    * shard provides a Map&lt;String, LongOpenHashSet&gt; of (groupKey → distinct values). The
2551:    * coordinator unions sets across shards per group and computes the final distinct count.
2552:    */

---

## Method 5: mergeDedupCountDistinctViaVarcharSets (Line 2553) — VARCHAR HashSet Merge (GROUP BY + COUNT DISTINCT via Sets)

2553:   private static List<Page> mergeDedupCountDistinctViaVarcharSets(
2554:       ShardExecuteResponse[] shardResults,
2555:       AggregationNode singleAgg,
2556:       List<Type> columnTypes,
2557:       Map<String, Type> columnTypeMap,
2558:       long topN) {
2559: 
2560:     // Phase 1: Collect per-group per-shard set sizes for top-K pruning
2561:     // Gather all group keys and their per-shard set references
2562:     int numShards = shardResults.length;
2563:     java.util.Map<String, org.opensearch.sql.dqe.operator.LongOpenHashSet[]> perGroupSets =
2564:         new java.util.HashMap<>();
2565: 
2566:     for (int s = 0; s < numShards; s++) {
2567:       java.util.Map<String, org.opensearch.sql.dqe.operator.LongOpenHashSet> shardSets =
2568:           shardResults[s].getVarcharDistinctSets();
2569:       if (shardSets == null) continue;
2570:       for (java.util.Map.Entry<String, org.opensearch.sql.dqe.operator.LongOpenHashSet> e :
2571:           shardSets.entrySet()) {
2572:         org.opensearch.sql.dqe.operator.LongOpenHashSet[] arr =
2573:             perGroupSets.computeIfAbsent(
2574:                 e.getKey(), k -> new org.opensearch.sql.dqe.operator.LongOpenHashSet[numShards]);
2575:         arr[s] = e.getValue();
2576:       }
2577:     }
2578: 
2579:     int totalGroups = perGroupSets.size();
2580: 
2581:     // Phase 2: Top-K pruning — only merge groups that could be in the top-K
2582:     // For each group: upper_bound = sum(shard_sizes), lower_bound = max(shard_sizes)
2583:     // Build min-heap of top-K lower bounds as pruning threshold
2584:     java.util.Set<String> candidateGroups;
2585:     if (topN > 0 && topN * 10 < totalGroups) {
2586:       // Compute bounds
2587:       int[] upperBounds = new int[totalGroups];
2588:       int[] lowerBounds = new int[totalGroups];
2589:       String[] groupKeys = new String[totalGroups];
2590:       int gi = 0;
2591:       for (java.util.Map.Entry<String, org.opensearch.sql.dqe.operator.LongOpenHashSet[]> e :
2592:           perGroupSets.entrySet()) {
2593:         groupKeys[gi] = e.getKey();
2594:         int ub = 0, lb = 0;
2595:         for (org.opensearch.sql.dqe.operator.LongOpenHashSet set : e.getValue()) {
2596:           if (set != null) {
2597:             ub += set.size();
2598:             if (set.size() > lb) lb = set.size();
2599:           }
2600:         }
2601:         upperBounds[gi] = ub;
2602:         lowerBounds[gi] = lb;
2603:         gi++;
2604:       }
2605: 
2606:       // Build min-heap of top-K lower bounds
2607:       java.util.PriorityQueue<Integer> minHeap = new java.util.PriorityQueue<>();
2608:       for (int i = 0; i < totalGroups; i++) {
2609:         minHeap.offer(lowerBounds[i]);
2610:         if (minHeap.size() > topN) minHeap.poll();
2611:       }
2612:       int threshold = minHeap.isEmpty() ? 0 : minHeap.peek();
2613: 
2614:       // Select candidates: groups where upper_bound >= threshold
2615:       candidateGroups = new java.util.HashSet<>();
2616:       for (int i = 0; i < totalGroups; i++) {
2617:         if (upperBounds[i] >= threshold) {
2618:           candidateGroups.add(groupKeys[i]);
2619:         }
2620:       }
2621:     } else {
2622:       candidateGroups = perGroupSets.keySet();
2623:     }
2624: 
2625:     // Phase 3: Merge only candidate groups
2626:     java.util.Map<String, org.opensearch.sql.dqe.operator.LongOpenHashSet> mergedSets =
2627:         new java.util.HashMap<>(candidateGroups.size() * 2);
2628: 
2629:     for (String key : candidateGroups) {
2630:       org.opensearch.sql.dqe.operator.LongOpenHashSet[] shardSets = perGroupSets.get(key);
2631:       // Take ownership of the largest shard's set
2632:       org.opensearch.sql.dqe.operator.LongOpenHashSet merged = null;
2633:       int mergedIdx = -1;
2634:       for (int s = 0; s < numShards; s++) {
2635:         if (shardSets[s] != null && (merged == null || shardSets[s].size() > merged.size())) {
2636:           merged = shardSets[s];
2637:           mergedIdx = s;
2638:         }
2639:       }
2640:       if (merged == null) continue;
2641: 
2642:       // Union remaining shards into the largest
2643:       for (int s = 0; s < numShards; s++) {
2644:         if (s == mergedIdx || shardSets[s] == null) continue;
2645:         org.opensearch.sql.dqe.operator.LongOpenHashSet src = shardSets[s];
2646:         long[] srcKeys = src.keys();
2647:         long empty = src.emptyMarker();
2648:         for (long v : srcKeys) {
2649:           if (v != empty) merged.add(v);
2650:         }
2651:         if (src.hasZeroValue()) merged.add(0L);
2652:         if (src.hasSentinelValue()) merged.add(Long.MIN_VALUE);
2653:       }
2654:       mergedSets.put(key, merged);
2655:     }
2656: 
2657:     int mergedGroupCount = mergedSets.size();
2658:     if (mergedGroupCount == 0) {
2659:       return List.of();
2660:     }
2661: 
2662:     // Phase 2: Build result with optional top-K pruning
2663:     // If topN > 0 and much smaller than totalGroups, use a min-heap to select top-K
2664:     String groupByKeyName = singleAgg.getGroupByKeys().get(0);
2665:     Type keyType =
2666:         columnTypeMap.getOrDefault(groupByKeyName, io.trino.spi.type.VarcharType.VARCHAR);
2667: 
2668:     // Collect (key, distinctCount) entries
2669:     java.util.List<java.util.Map.Entry<String, org.opensearch.sql.dqe.operator.LongOpenHashSet>>
2670:         entries = new java.util.ArrayList<>(mergedSets.entrySet());
2671: 
2672:     // Build output: 2 columns (groupKey VARCHAR, count BIGINT)
2673:     int outputSize = (topN > 0 && topN < mergedGroupCount) ? (int) topN : mergedGroupCount;
2674: 
2675:     if (topN > 0 && topN < mergedGroupCount) {
2676:       // Partial sort: only need top-K by count DESC
2677:       entries.sort((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()));
2678:       entries = entries.subList(0, outputSize);
2679:     }
2680: 
2681:     io.trino.spi.block.BlockBuilder keyBuilder =
2682:         io.trino.spi.type.VarcharType.VARCHAR.createBlockBuilder(null, outputSize);
2683:     io.trino.spi.block.BlockBuilder countBuilder =
2684:         io.trino.spi.type.BigintType.BIGINT.createBlockBuilder(null, outputSize);
2685: 
2686:     for (java.util.Map.Entry<String, org.opensearch.sql.dqe.operator.LongOpenHashSet> e : entries) {
2687:       io.airlift.slice.Slice keySlice = io.airlift.slice.Slices.utf8Slice(e.getKey());
2688:       io.trino.spi.type.VarcharType.VARCHAR.writeSlice(keyBuilder, keySlice);
2689:       io.trino.spi.type.BigintType.BIGINT.writeLong(countBuilder, e.getValue().size());
2690:     }
2691: 
2692:     // Output column types: match the coordinator aggregation output schema
2693:     // singleAgg outputs: [groupByKey, COUNT(DISTINCT col)]
2694:     return List.of(new Page(keyBuilder.build(), countBuilder.build()));
2695:   }
2696: 

---

## BONUS: mergeDedupCountDistinct2Key (Line 2697) — Ultra-fast 2-Key Fused Merge

2697:   private static List<Page> mergeDedupCountDistinct2Key(
2698:       List<List<Page>> shardPages,
2699:       List<Type> dedupTypes,
2700:       int numOriginalKeys,
2701:       int numCountDistinctAggs) {
2702:     // Phase 0: Batch all shard data into flat long arrays to eliminate Block virtual dispatch.
2703:     int totalRows = 0;
2704:     int maxShardRows = 0;
2705:     for (List<Page> shardPageList : shardPages) {
2706:       int shardRows = 0;
2707:       for (Page page : shardPageList) shardRows += page.getPositionCount();
2708:       totalRows += shardRows;
2709:       if (shardRows > maxShardRows) maxShardRows = shardRows;
2710:     }
2711:     if (totalRows == 0) return List.of();
2712: 
2713:     long[] allKey0 = new long[totalRows];
2714:     long[] allKey1 = new long[totalRows];
2715:     int off = 0;
2716:     Type type0 = dedupTypes.get(0);
2717:     Type type1 = dedupTypes.get(1);
2718:     for (List<Page> shardPageList : shardPages) {
2719:       for (Page page : shardPageList) {
2720:         int cnt = page.getPositionCount();
2721:         Block b0 = page.getBlock(0);
2722:         Block b1 = page.getBlock(1);
2723:         for (int p = 0; p < cnt; p++) {
2724:           allKey0[off] = type0.getLong(b0, p);
2725:           allKey1[off] = type1.getLong(b1, p);
2726:           off++;
2727:         }
2728:       }
2729:     }
2730: 
2731:     // Phase 1+2 fused: Dedup (k0, k1) pairs and count per k0 in single pass.
2732:     int estimatedUnique = Math.min(totalRows, maxShardRows * 2);
2733:     int capacity = Integer.highestOneBit(Math.max(1024, (int) (estimatedUnique / 0.65f))) << 1;
2734:     float loadFactor = 0.65f;
2735:     int threshold = (int) (capacity * loadFactor);
2736:     int size = 0;
2737:     long[] mapKey0 = new long[capacity];
2738:     long[] mapKey1 = new long[capacity];
2739:     boolean[] mapOccupied = new boolean[capacity];
2740: 
2741:     // Group counter map (maintained incrementally during dedup)
2742:     int grpCap = 256;
2743:     long[] grpKeys = new long[grpCap];
2744:     long[] grpCounts = new long[grpCap];
2745:     boolean[] grpOcc = new boolean[grpCap];
2746:     int grpSize = 0;
2747:     int grpThreshold = (int) (grpCap * 0.7f);
2748: 
2749:     for (int i = 0; i < totalRows; i++) {
2750:       long k0 = allKey0[i];
2751:       long k1 = allKey1[i];
2752:       int hash = Long.hashCode(k0) * 0x9E3779B9 + Long.hashCode(k1);
2753:       int mask = capacity - 1;
2754:       int slot = hash & mask;
2755:       boolean isNew = false;
2756:       while (true) {
2757:         if (!mapOccupied[slot]) {
2758:           mapKey0[slot] = k0;
2759:           mapKey1[slot] = k1;
2760:           mapOccupied[slot] = true;
2761:           size++;
2762:           isNew = true;
2763:           if (size > threshold) {
2764:             int newCap = capacity * 2;
2765:             long[] nk0 = new long[newCap];
2766:             long[] nk1 = new long[newCap];
2767:             boolean[] no = new boolean[newCap];
2768:             int nm = newCap - 1;
2769:             for (int s = 0; s < capacity; s++) {
2770:               if (mapOccupied[s]) {
2771:                 int h = Long.hashCode(mapKey0[s]) * 0x9E3779B9 + Long.hashCode(mapKey1[s]);
2772:                 int ns = h & nm;
2773:                 while (no[ns]) ns = (ns + 1) & nm;
2774:                 nk0[ns] = mapKey0[s];
2775:                 nk1[ns] = mapKey1[s];
2776:                 no[ns] = true;
2777:               }
2778:             }
2779:             mapKey0 = nk0;
2780:             mapKey1 = nk1;
2781:             mapOccupied = no;
2782:             capacity = newCap;
2783:             mask = nm;
2784:             threshold = (int) (newCap * loadFactor);
2785:           }
2786:           break;
2787:         }
2788:         if (mapKey0[slot] == k0 && mapKey1[slot] == k1) {
2789:           break; // duplicate
2790:         }
2791:         slot = (slot + 1) & mask;
2792:       }
2793: 
2794:       // Fused: increment group counter for k0 when a new unique pair is found
2795:       if (isNew) {
2796:         int gm = grpCap - 1;
2797:         int gs = Long.hashCode(k0) & gm;
2798:         while (grpOcc[gs] && grpKeys[gs] != k0) gs = (gs + 1) & gm;
2799:         if (grpOcc[gs]) {
2800:           grpCounts[gs]++;
2801:         } else {
2802:           grpKeys[gs] = k0;
2803:           grpCounts[gs] = 1;
2804:           grpOcc[gs] = true;
2805:           grpSize++;
2806:           if (grpSize > grpThreshold) {
2807:             int newGC = grpCap * 2;
2808:             long[] ngk = new long[newGC];
2809:             long[] ngc = new long[newGC];
2810:             boolean[] ngo = new boolean[newGC];
2811:             int ngm = newGC - 1;
2812:             for (int g = 0; g < grpCap; g++) {
2813:               if (grpOcc[g]) {
2814:                 int ns = Long.hashCode(grpKeys[g]) & ngm;
2815:                 while (ngo[ns]) ns = (ns + 1) & ngm;
2816:                 ngk[ns] = grpKeys[g];
2817:                 ngc[ns] = grpCounts[g];
2818:                 ngo[ns] = true;
2819:               }
2820:             }
2821:             grpKeys = ngk;
2822:             grpCounts = ngc;
2823:             grpOcc = ngo;
2824:             grpCap = newGC;
2825:             grpThreshold = (int) (newGC * 0.7f);
2826:           }
2827:         }
2828:       }
2829:     }
2830: 
2831:     if (grpSize == 0) return List.of();
2832: 
2833:     // Build result page
2834:     Type keyType = dedupTypes.get(0);
2835:     int numOutputCols = numOriginalKeys + numCountDistinctAggs;
2836:     io.trino.spi.block.BlockBuilder[] builders = new io.trino.spi.block.BlockBuilder[numOutputCols];
2837:     builders[0] = keyType.createBlockBuilder(null, grpSize);
2838:     for (int i = numOriginalKeys; i < numOutputCols; i++) {
2839:       builders[i] = BigintType.BIGINT.createBlockBuilder(null, grpSize);
2840:     }
2841: 
2842:     for (int gs = 0; gs < grpCap; gs++) {
2843:       if (!grpOcc[gs]) continue;
2844:       long key = grpKeys[gs];
2845:       if (keyType instanceof IntegerType) {
2846:         IntegerType.INTEGER.writeLong(builders[0], (int) key);
2847:       } else {
2848:         keyType.writeLong(builders[0], key);
2849:       }
2850:       for (int i = numOriginalKeys; i < numOutputCols; i++) {
2851:         BigintType.BIGINT.writeLong(builders[i], grpCounts[gs]);
2852:       }
2853:     }
2854: 
2855:     Block[] blocks = new Block[numOutputCols];
2856:     for (int i = 0; i < numOutputCols; i++) {
2857:       blocks[i] = builders[i].build();
2858:     }
2859:     return List.of(new Page(blocks));
2860:   }
2861: 
2862:   /**
2863:    * Check if the shard plan uses mixed-aggregate dedup. Returns true when the shard plan is a
2864:    * PARTIAL AggregationNode whose GROUP BY keys are a superset of the original SINGLE aggregation's
2865:    * GROUP BY keys, AND the shard has more aggregate functions than just COUNT(*) (distinguishing
2866:    * from the COUNT-DISTINCT-only dedup path).
2867:    */
2868: 
2869:   /**
2870:    * Ultra-fast 2-key fused merge for mixed-aggregate dedup queries (Q10-style). Uses two flat
2871:    * long[] arrays instead of long[][] to eliminate per-entry allocation. Combines Stage 1 (dedup
2872:    * merge) and Stage 2 (re-aggregate by original key) into a single pass.
2873:    */
2874: 
2875:   /**
2876:    * Merge mixed-dedup results using attached HashSets (Q10 optimization). Each shard provides ~400
2877:    * groups with SUM/COUNT accumulators + per-group LongOpenHashSets, instead of ~25K expanded
2878:    * (key0, key1) rows. Merges accumulators + unions HashSets across shards.

---

## Dispatch Logic (which query routes to which method)

| Call Site | Line | Method Called | Query Pattern |
|-----------|------|--------------|---------------|
| Line 629 | Scalar numeric | `mergeCountDistinctValuesViaRawSets` | Q04: `SELECT COUNT(DISTINCT numericCol) FROM t` |
| Line 633 | Scalar varchar | `mergeCountDistinctVarcharViaRawSets` | Q05: `SELECT COUNT(DISTINCT varcharCol) FROM t` |
| Line 660 | Grouped varchar+sets | `mergeDedupCountDistinctViaVarcharSets` | Q13: `GROUP BY varcharCol COUNT(DISTINCT numericCol)` with attached sets |
| Line 674 | Grouped numeric+sets | `mergeDedupCountDistinctViaSets` | Q08/Q09/Q11: `GROUP BY numericCol COUNT(DISTINCT numericCol)` with attached sets |
| Line 680 | Grouped fallback | `mergeDedupCountDistinct` | Generic grouped COUNT(DISTINCT) without attached sets |
| Line 2161 | 2-key fast path | `mergeDedupCountDistinct2Key` | Called from within `mergeDedupCountDistinct` for 2 numeric dedup keys |
