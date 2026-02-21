/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.exchange;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Merge-sorts pre-sorted shard results into a globally sorted output.
 *
 * <p>Used for TopK queries (sort + limit). Each shard independently sorts and limits locally; the
 * coordinator merge-sorts the pre-sorted shard results using a priority queue.
 *
 * <p>The limit is applied after the merge to produce the final top-K rows.
 */
public class MergeSortExchange extends Exchange {

  private final List<RelFieldCollation> collations;
  private final int limit;
  private List<Object[]> mergedRows = Collections.emptyList();

  /**
   * Creates a MergeSortExchange.
   *
   * @param shardPlan the plan fragment to execute on each shard
   * @param rowType the output row type
   * @param collations the sort keys defining the global order
   * @param limit the maximum number of rows to return (applied after merge)
   */
  public MergeSortExchange(
      RelNode shardPlan, RelDataType rowType, List<RelFieldCollation> collations, int limit) {
    super(shardPlan, rowType);
    this.collations = collations;
    this.limit = limit;
  }

  @Override
  public void setShardResults(List<ShardResult> results) {
    Comparator<Object[]> comparator = buildComparator(collations);

    // Use a priority queue entry that tracks the shard iterator it came from
    PriorityQueue<ShardIteratorEntry> pq = new PriorityQueue<>(Math.max(1, results.size()));

    for (ShardResult result : results) {
      Iterator<Object[]> iter = result.getRows().iterator();
      if (iter.hasNext()) {
        pq.offer(new ShardIteratorEntry(iter.next(), iter, comparator, result.getShardId()));
      }
    }

    List<Object[]> sorted = new ArrayList<>();
    while (!pq.isEmpty() && (limit <= 0 || sorted.size() < limit)) {
      ShardIteratorEntry entry = pq.poll();
      sorted.add(entry.currentRow);
      if (entry.iterator.hasNext()) {
        pq.offer(
            new ShardIteratorEntry(entry.iterator.next(), entry.iterator, comparator, entry.shardId));
      }
    }

    this.mergedRows = sorted;
  }

  @Override
  public Iterator<Object[]> scan() {
    return mergedRows.iterator();
  }

  /** Returns the sort collations. */
  public List<RelFieldCollation> getCollations() {
    return collations;
  }

  /** Returns the limit. */
  public int getLimit() {
    return limit;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static Comparator<Object[]> buildComparator(List<RelFieldCollation> collations) {
    Comparator<Object[]> comparator = null;
    for (RelFieldCollation collation : collations) {
      int index = collation.getFieldIndex();
      Direction direction = collation.getDirection();
      NullDirection nullDirection = collation.nullDirection;

      Comparator<Object[]> fieldComparator =
          (row1, row2) -> {
            Object v1 = row1[index];
            Object v2 = row2[index];

            // Handle nulls
            if (v1 == null && v2 == null) {
              return 0;
            }
            if (v1 == null) {
              return nullFirst(nullDirection, direction) ? -1 : 1;
            }
            if (v2 == null) {
              return nullFirst(nullDirection, direction) ? 1 : -1;
            }

            int cmp = ((Comparable) v1).compareTo(v2);
            return (direction == Direction.DESCENDING) ? -cmp : cmp;
          };

      comparator = (comparator == null) ? fieldComparator : comparator.thenComparing(fieldComparator);
    }
    return comparator != null ? comparator : (a, b) -> 0;
  }

  /**
   * Determines whether NULLs should be ordered first.
   *
   * <p>Calcite's NullDirection semantics: FIRST means nulls come first, LAST means nulls come last,
   * UNSPECIFIED uses the default for the direction (LAST for ASC, FIRST for DESC in most
   * databases).
   */
  private static boolean nullFirst(NullDirection nullDirection, Direction direction) {
    if (nullDirection == NullDirection.FIRST) {
      return true;
    }
    if (nullDirection == NullDirection.LAST) {
      return false;
    }
    // UNSPECIFIED: default is NULLS LAST for ASC, NULLS FIRST for DESC
    return direction == Direction.DESCENDING;
  }

  /**
   * Priority queue entry that wraps a current row, the iterator it came from, and the shard ID.
   * When the primary comparator considers two rows equal, shard ID is used as a tiebreaker to
   * produce deterministic ordering across distributed executions.
   */
  private static class ShardIteratorEntry implements Comparable<ShardIteratorEntry> {
    final Object[] currentRow;
    final Iterator<Object[]> iterator;
    final Comparator<Object[]> comparator;
    final int shardId;

    ShardIteratorEntry(
        Object[] currentRow,
        Iterator<Object[]> iterator,
        Comparator<Object[]> comparator,
        int shardId) {
      this.currentRow = currentRow;
      this.iterator = iterator;
      this.comparator = comparator;
      this.shardId = shardId;
    }

    @Override
    public int compareTo(ShardIteratorEntry other) {
      int cmp = comparator.compare(this.currentRow, other.currentRow);
      if (cmp != 0) {
        return cmp;
      }
      // Tiebreaker: lower shard ID first for deterministic ordering
      return Integer.compare(this.shardId, other.shardId);
    }
  }
}
