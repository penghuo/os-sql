/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TTL-based cache for {@link DqeTableStatistics}. Entries expire after a configurable time-to-live
 * and are evicted lazily on the next {@code get()} call. Thread-safe via {@link ConcurrentHashMap}.
 *
 * <p>The cache is keyed by index name. When the generation stored in a cached entry is older than
 * the generation provided at lookup time, the entry is considered stale and evicted.
 */
public class StatisticsCache {

  private final long ttlMillis;
  private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();

  /**
   * Creates a new statistics cache.
   *
   * @param ttlMillis time-to-live in milliseconds for cached entries
   */
  public StatisticsCache(long ttlMillis) {
    if (ttlMillis <= 0) {
      throw new IllegalArgumentException("ttlMillis must be positive, got: " + ttlMillis);
    }
    this.ttlMillis = ttlMillis;
  }

  /**
   * Returns cached statistics for the given index, or {@link DqeTableStatistics#UNKNOWN} if not
   * cached, expired, or stale (generation mismatch).
   *
   * @param indexName the index name
   * @param currentGeneration the current schema generation for staleness check
   * @return cached statistics or UNKNOWN
   */
  public DqeTableStatistics get(String indexName, long currentGeneration) {
    Objects.requireNonNull(indexName, "indexName must not be null");
    CacheEntry entry = cache.get(indexName);
    if (entry == null) {
      return DqeTableStatistics.UNKNOWN;
    }
    long now = System.currentTimeMillis();
    if (now - entry.createdAtMillis > ttlMillis) {
      cache.remove(indexName);
      return DqeTableStatistics.UNKNOWN;
    }
    if (entry.stats.getGeneration() != currentGeneration) {
      cache.remove(indexName);
      return DqeTableStatistics.UNKNOWN;
    }
    return entry.stats;
  }

  /**
   * Stores statistics for the given index.
   *
   * @param indexName the index name
   * @param stats the statistics to cache
   */
  public void put(String indexName, DqeTableStatistics stats) {
    Objects.requireNonNull(indexName, "indexName must not be null");
    Objects.requireNonNull(stats, "stats must not be null");
    cache.put(indexName, new CacheEntry(stats, System.currentTimeMillis()));
  }

  /** Invalidates the cached entry for the given index. */
  public void invalidate(String indexName) {
    Objects.requireNonNull(indexName, "indexName must not be null");
    cache.remove(indexName);
  }

  /** Invalidates all cached entries. */
  public void invalidateAll() {
    cache.clear();
  }

  /** Returns the number of entries currently in the cache (including potentially expired ones). */
  public int size() {
    return cache.size();
  }

  private static final class CacheEntry {
    final DqeTableStatistics stats;
    final long createdAtMillis;

    CacheEntry(DqeTableStatistics stats, long createdAtMillis) {
      this.stats = stats;
      this.createdAtMillis = createdAtMillis;
    }
  }
}
