/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.datafusion;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * JNI bridge to DataFusion (Rust) for vectorized GROUP BY + aggregation. Arrays are passed directly
 * to native code; the JNI layer handles pinning/copying.
 */
public final class DataFusionBridge {

  private static volatile boolean loaded = false;
  private static volatile boolean available = false;

  /** Load the native library from classpath resources. */
  public static synchronized void ensureLoaded() {
    if (loaded) return;
    loaded = true;
    try {
      // Try java.library.path first
      System.loadLibrary("dqe_datafusion");
      available = true;
    } catch (UnsatisfiedLinkError e1) {
      // Extract from resources to temp file
      try (InputStream is = DataFusionBridge.class.getResourceAsStream("/libdqe_datafusion.so")) {
        if (is == null) return; // not bundled — DataFusion not available
        Path tmp = Files.createTempFile("libdqe_datafusion", ".so");
        Files.copy(is, tmp, StandardCopyOption.REPLACE_EXISTING);
        System.load(tmp.toAbsolutePath().toString());
        tmp.toFile().deleteOnExit();
        available = true;
      } catch (Exception e2) {
        // Swallow — DataFusion acceleration not available
      }
    }
  }

  /** Returns true if the native library was loaded successfully. */
  public static boolean isAvailable() {
    ensureLoaded();
    return available;
  }

  /**
   * GROUP BY key (long[]) with COUNT(DISTINCT value) (long[]). Returns flat long[]: [key0, count0,
   * key1, count1, ...] sorted by count DESC.
   *
   * @param keys GROUP BY key column
   * @param values COUNT(DISTINCT) argument column
   * @param numRows number of rows
   * @param topN limit (0 = all groups)
   */
  public static native long[] groupByCountDistinct(
      long[] keys, long[] values, int numRows, int topN);

  /**
   * GROUP BY key (long[]) with mixed aggregations.
   *
   * @param keys GROUP BY key column
   * @param numRows number of rows
   * @param aggArrays array of jlongArray references for each agg column
   * @param aggTypes 0=SUM, 1=COUNT_STAR, 2=AVG, 3=COUNT_DISTINCT
   * @param numAggs number of aggregate functions
   * @param topN limit (0 = all groups)
   * @param sortAggIdx which aggregate to sort by DESC (-1 = sort by key)
   * @return flat long[]: [key, a0, a1, ..., key, a0, a1, ...]
   */
  public static native long[] groupByMixedAgg(
      long[] keys,
      int numRows,
      long[] aggArrays,
      long[] aggTypes,
      int numAggs,
      int topN,
      int sortAggIdx);

  private DataFusionBridge() {}
}
