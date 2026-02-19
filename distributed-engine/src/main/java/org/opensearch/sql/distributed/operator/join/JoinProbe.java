/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.join;

import java.util.Objects;
import org.opensearch.sql.distributed.data.Page;

/**
 * Probes a {@link JoinHash} for a single probe-side row. Iterates through all matching build-side
 * positions for the current probe row.
 *
 * <p>Ported from Trino's io.trino.operator.join.JoinProbe (simplified).
 */
public class JoinProbe {

  private final JoinHash joinHash;
  private final int[] probeKeyChannels;
  private final Page probePage;
  private final int probePosition;

  private final int[] matchingPositions;
  private int matchIndex;

  /**
   * Creates a new JoinProbe for a single probe row.
   *
   * @param joinHash the build-side hash table
   * @param probeKeyChannels column indices in the probe page for join keys
   * @param probePage the probe-side page
   * @param probePosition the row position within the probe page to probe
   */
  public JoinProbe(JoinHash joinHash, int[] probeKeyChannels, Page probePage, int probePosition) {
    this.joinHash = Objects.requireNonNull(joinHash, "joinHash is null");
    this.probeKeyChannels = Objects.requireNonNull(probeKeyChannels, "probeKeyChannels is null");
    this.probePage = Objects.requireNonNull(probePage, "probePage is null");
    this.probePosition = probePosition;

    this.matchingPositions =
        joinHash.getMatchingPositions(probePage, probePosition, probeKeyChannels);
    this.matchIndex = 0;
  }

  /** Returns true if there is another matching build-side position. */
  public boolean hasNextMatch() {
    return matchIndex < matchingPositions.length;
  }

  /**
   * Returns the flat position index of the current match, and advances to the next match.
   *
   * @throws IllegalStateException if there are no more matches
   */
  public int nextMatch() {
    if (!hasNextMatch()) {
      throw new IllegalStateException("No more matching positions");
    }
    return matchingPositions[matchIndex++];
  }

  /** Returns the number of matching build-side rows. */
  public int getMatchCount() {
    return matchingPositions.length;
  }

  /** Returns the probe page. */
  public Page getProbePage() {
    return probePage;
  }

  /** Returns the probe position. */
  public int getProbePosition() {
    return probePosition;
  }
}
