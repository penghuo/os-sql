/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.join;

import java.util.Objects;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.Page;

/**
 * Hash table for join lookups. Wraps {@link PagesHash} and provides a high-level interface for the
 * {@link LookupJoinOperator} to probe build-side rows.
 *
 * <p>Ported from Trino's io.trino.operator.join.JoinHash (simplified).
 */
public class JoinHash {

  private final PagesHash pagesHash;

  public JoinHash(PagesHash pagesHash) {
    this.pagesHash = Objects.requireNonNull(pagesHash, "pagesHash is null");
  }

  /**
   * Finds all build-side flat position indices matching the given probe row.
   *
   * @param probePage the probe-side page
   * @param probePosition the row position within the probe page
   * @param probeKeyChannels column indices in the probe page for the join keys
   * @return array of matching build-side flat position indices (may be empty)
   */
  public int[] getMatchingPositions(Page probePage, int probePosition, int[] probeKeyChannels) {
    return pagesHash.getMatchingPositions(probePage, probePosition, probeKeyChannels);
  }

  /** Returns the total number of build-side positions. */
  public int getPositionCount() {
    return pagesHash.getPositionCount();
  }

  /** Returns the build-side page at the given page index. */
  public Page getPage(int pageIndex) {
    return pagesHash.getPage(pageIndex);
  }

  /** Returns the page index for the given flat position. */
  public int getPageIndex(int flatPosition) {
    return pagesHash.getPageIndex(flatPosition);
  }

  /** Returns the position within the page for the given flat position. */
  public int getPositionInPage(int flatPosition) {
    return pagesHash.getPositionInPage(flatPosition);
  }

  /** Returns the block at the given channel for the given flat position. */
  public Block getBlockForPosition(int flatPosition, int channel) {
    return pagesHash.getBlockForPosition(flatPosition, channel);
  }

  /** Returns the number of channels in build-side pages. */
  public int getChannelCount() {
    return pagesHash.getChannelCount();
  }

  /** Returns the number of build-side pages. */
  public int getPageCount() {
    return pagesHash.getPageCount();
  }

  /** Returns the estimated memory usage in bytes. */
  public long getEstimatedSizeInBytes() {
    return pagesHash.getEstimatedSizeInBytes();
  }
}
