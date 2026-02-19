/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.join;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;

/**
 * Tests for LookupJoinOperator: INNER, LEFT, RIGHT, FULL, SEMI, ANTI joins, multi-key, null keys,
 * empty sides.
 */
class LookupJoinOperatorTest {

  private static final OperatorContext CTX = new OperatorContext(0, "LookupJoin");

  // --- Helper methods ---

  /** Builds a JoinHash from build-side key/value arrays. */
  private JoinHash buildHash(long[] keys, long[] values) {
    HashBuilderOperator builder =
        new HashBuilderOperator(new OperatorContext(1, "HashBuilder"), new int[] {0});
    builder.addInput(
        new Page(
            new LongArrayBlock(keys.length, Optional.empty(), keys),
            new LongArrayBlock(values.length, Optional.empty(), values)));
    builder.finish();
    return builder.getJoinHash();
  }

  /** Builds a JoinHash with nullable keys. */
  private JoinHash buildHashWithNulls(long[] keys, boolean[] keyNulls, long[] values) {
    HashBuilderOperator builder =
        new HashBuilderOperator(new OperatorContext(1, "HashBuilder"), new int[] {0});
    builder.addInput(
        new Page(
            new LongArrayBlock(keys.length, Optional.of(keyNulls), keys),
            new LongArrayBlock(values.length, Optional.empty(), values)));
    builder.finish();
    return builder.getJoinHash();
  }

  /** Builds a JoinHash with no rows. */
  private JoinHash buildEmptyHash() {
    HashBuilderOperator builder =
        new HashBuilderOperator(new OperatorContext(1, "HashBuilder"), new int[] {0});
    builder.finish();
    return builder.getJoinHash();
  }

  /** Creates a probe page with key and value columns. */
  private Page probePage(long[] keys, long[] values) {
    return new Page(
        new LongArrayBlock(keys.length, Optional.empty(), keys),
        new LongArrayBlock(values.length, Optional.empty(), values));
  }

  /** Creates a probe page with nullable keys. */
  private Page probePageWithNulls(long[] keys, boolean[] keyNulls, long[] values) {
    return new Page(
        new LongArrayBlock(keys.length, Optional.of(keyNulls), keys),
        new LongArrayBlock(values.length, Optional.empty(), values));
  }

  /** Collects (probeVal, buildVal) pairs from an INNER/LEFT/RIGHT/FULL join output. */
  private Map<Long, Set<Long>> collectJoinPairs(Page output) {
    Map<Long, Set<Long>> result = new HashMap<>();
    LongArrayBlock probeVals = (LongArrayBlock) output.getBlock(0);
    LongArrayBlock buildVals = (LongArrayBlock) output.getBlock(1);
    for (int i = 0; i < output.getPositionCount(); i++) {
      long pv = probeVals.isNull(i) ? -999 : probeVals.getLong(i);
      long bv = buildVals.isNull(i) ? -999 : buildVals.getLong(i);
      result.computeIfAbsent(pv, k -> new HashSet<>()).add(bv);
    }
    return result;
  }

  // ==================== INNER JOIN ====================

  @Test
  @DisplayName("INNER join: matching rows produce output")
  void innerJoinMatchingRows() {
    // Build: key=1->100, key=2->200
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    // Probe: key=1->10, key=2->20, key=3->30
    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX,
            hash,
            JoinType.INNER,
            new int[] {0}, // probeKeyChannels
            new int[] {1}, // probeOutputChannels (value only)
            new int[] {1}); // buildOutputChannels (value only)

    op.addInput(probePage(new long[] {1, 2, 3}, new long[] {10, 20, 30}));
    Page output = op.getOutput();

    assertNotNull(output);
    assertEquals(2, output.getPositionCount(), "Only keys 1,2 match; key 3 has no match");

    // Verify output has probe value and build value columns
    assertEquals(2, output.getChannelCount());

    op.finish();
    assertTrue(op.isFinished());
    op.close();
  }

  @Test
  @DisplayName("INNER join: no matching rows produce empty output")
  void innerJoinNoMatches() {
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.INNER, new int[] {0}, new int[] {1}, new int[] {1});

    op.addInput(probePage(new long[] {9, 10}, new long[] {90, 100}));
    Page output = op.getOutput();

    assertNull(output, "No matching keys should produce null output");

    op.finish();
    assertTrue(op.isFinished());
    op.close();
  }

  @Test
  @DisplayName("INNER join: duplicate build keys produce multiple rows")
  void innerJoinDuplicateBuildKeys() {
    // Build: key=1->100, key=1->200
    JoinHash hash = buildHash(new long[] {1, 1}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.INNER, new int[] {0}, new int[] {1}, new int[] {1});

    op.addInput(probePage(new long[] {1}, new long[] {10}));
    Page output = op.getOutput();

    assertNotNull(output);
    assertEquals(2, output.getPositionCount(), "Probe key=1 matches 2 build rows");

    op.close();
  }

  // ==================== LEFT JOIN ====================

  @Test
  @DisplayName("LEFT join: unmatched probe rows get null build columns")
  void leftJoinUnmatchedProbe() {
    JoinHash hash = buildHash(new long[] {1}, new long[] {100});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.LEFT, new int[] {0}, new int[] {1}, new int[] {1});

    op.addInput(probePage(new long[] {1, 2}, new long[] {10, 20}));
    Page output = op.getOutput();

    assertNotNull(output);
    assertEquals(2, output.getPositionCount(), "LEFT join keeps all probe rows");

    // Second row (key=2, no match) should have null build value
    LongArrayBlock buildVals = (LongArrayBlock) output.getBlock(1);
    // Find which row is the unmatched one
    boolean foundNullBuild = false;
    for (int i = 0; i < output.getPositionCount(); i++) {
      if (buildVals.isNull(i)) {
        foundNullBuild = true;
      }
    }
    assertTrue(foundNullBuild, "Unmatched probe row should have null build value");

    op.finish();
    assertTrue(op.isFinished());
    op.close();
  }

  @Test
  @DisplayName("LEFT join: all probe rows match produces no nulls")
  void leftJoinAllMatch() {
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.LEFT, new int[] {0}, new int[] {1}, new int[] {1});

    op.addInput(probePage(new long[] {1, 2}, new long[] {10, 20}));
    Page output = op.getOutput();

    assertNotNull(output);
    assertEquals(2, output.getPositionCount());

    LongArrayBlock buildVals = (LongArrayBlock) output.getBlock(1);
    for (int i = 0; i < output.getPositionCount(); i++) {
      assertFalse(buildVals.isNull(i), "All rows match, no nulls expected");
    }

    op.close();
  }

  // ==================== RIGHT JOIN ====================

  @Test
  @DisplayName("RIGHT join: unmatched build rows emitted at finish")
  void rightJoinUnmatchedBuild() {
    // Build: key=1->100, key=2->200, key=3->300
    JoinHash hash = buildHash(new long[] {1, 2, 3}, new long[] {100, 200, 300});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.RIGHT, new int[] {0}, new int[] {1}, new int[] {1});

    // Probe only matches key=1
    op.addInput(probePage(new long[] {1}, new long[] {10}));
    Page matched = op.getOutput();
    assertNotNull(matched);
    assertEquals(1, matched.getPositionCount());

    // Finish should emit unmatched build rows (key=2, key=3)
    op.finish();
    assertFalse(op.isFinished(), "Should be in FINISHING state");

    Page unmatched = op.getOutput();
    assertTrue(op.isFinished());

    assertNotNull(unmatched);
    assertEquals(2, unmatched.getPositionCount(), "2 unmatched build rows");

    // Unmatched build rows should have null probe values
    LongArrayBlock probeVals = (LongArrayBlock) unmatched.getBlock(0);
    for (int i = 0; i < unmatched.getPositionCount(); i++) {
      assertTrue(probeVals.isNull(i), "Unmatched build rows should have null probe value");
    }

    op.close();
  }

  @Test
  @DisplayName("RIGHT join: all build rows matched produces no unmatched at finish")
  void rightJoinAllBuildMatched() {
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.RIGHT, new int[] {0}, new int[] {1}, new int[] {1});

    op.addInput(probePage(new long[] {1, 2}, new long[] {10, 20}));
    op.getOutput();

    op.finish();
    Page unmatched = op.getOutput();
    assertTrue(op.isFinished());
    assertNull(unmatched, "No unmatched build rows when all matched");

    op.close();
  }

  // ==================== FULL JOIN ====================

  @Test
  @DisplayName("FULL join: unmatched from both sides")
  void fullJoinUnmatchedBothSides() {
    // Build: key=1->100, key=2->200
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.FULL, new int[] {0}, new int[] {1}, new int[] {1});

    // Probe: key=1 matches, key=3 does not
    op.addInput(probePage(new long[] {1, 3}, new long[] {10, 30}));
    Page probeOutput = op.getOutput();

    assertNotNull(probeOutput);
    // key=1 matched, key=3 unmatched (with null build)
    assertEquals(2, probeOutput.getPositionCount());

    // Finish: unmatched build row key=2 emitted
    op.finish();
    Page buildOutput = op.getOutput();
    assertTrue(op.isFinished());

    assertNotNull(buildOutput);
    assertEquals(1, buildOutput.getPositionCount(), "Build key=2 was never matched");

    op.close();
  }

  // ==================== SEMI JOIN ====================

  @Test
  @DisplayName("SEMI join: returns probe rows with at least one match, no build columns")
  void semiJoinReturnsMatchedProbeOnly() {
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX,
            hash,
            JoinType.SEMI,
            new int[] {0},
            new int[] {0, 1}, // output both probe columns
            new int[] {}); // no build columns in output

    op.addInput(probePage(new long[] {1, 3, 2}, new long[] {10, 30, 20}));
    Page output = op.getOutput();

    assertNotNull(output);
    assertEquals(2, output.getPositionCount(), "Only probe rows with matches (key=1, key=2)");
    assertEquals(2, output.getChannelCount(), "Only probe columns in output");

    op.finish();
    assertTrue(op.isFinished());
    op.close();
  }

  @Test
  @DisplayName("SEMI join: duplicate build keys still produce single probe row")
  void semiJoinNoDuplicateOutput() {
    // Build: key=1 appears 3 times
    JoinHash hash = buildHash(new long[] {1, 1, 1}, new long[] {100, 200, 300});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.SEMI, new int[] {0}, new int[] {1}, new int[] {});

    op.addInput(probePage(new long[] {1}, new long[] {10}));
    Page output = op.getOutput();

    assertNotNull(output);
    assertEquals(
        1, output.getPositionCount(), "SEMI should produce exactly 1 row per matching probe");

    op.close();
  }

  // ==================== ANTI JOIN ====================

  @Test
  @DisplayName("ANTI join: returns probe rows with NO match, no build columns")
  void antiJoinReturnsUnmatchedProbeOnly() {
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX,
            hash,
            JoinType.ANTI,
            new int[] {0},
            new int[] {0, 1}, // output both probe columns
            new int[] {}); // no build columns

    op.addInput(probePage(new long[] {1, 3, 4}, new long[] {10, 30, 40}));
    Page output = op.getOutput();

    assertNotNull(output);
    assertEquals(2, output.getPositionCount(), "Only unmatched probe rows (key=3, key=4)");
    assertEquals(2, output.getChannelCount(), "Only probe columns in output");

    op.close();
  }

  @Test
  @DisplayName("ANTI join: all rows match produces empty output")
  void antiJoinAllMatch() {
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.ANTI, new int[] {0}, new int[] {1}, new int[] {});

    op.addInput(probePage(new long[] {1, 2}, new long[] {10, 20}));
    Page output = op.getOutput();

    assertNull(output, "All probe rows match, so ANTI join produces nothing");

    op.close();
  }

  // ==================== NULL KEY HANDLING ====================

  @Test
  @DisplayName("Null probe keys produce no matches in INNER join")
  void nullProbeKeysInnerJoin() {
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.INNER, new int[] {0}, new int[] {1}, new int[] {1});

    Page probe =
        probePageWithNulls(
            new long[] {1, 0, 2}, new boolean[] {false, true, false}, new long[] {10, 20, 30});
    op.addInput(probe);
    Page output = op.getOutput();

    assertNotNull(output);
    // Only key=1 and key=2 match; null key does not match
    assertEquals(2, output.getPositionCount());

    op.close();
  }

  @Test
  @DisplayName("Null probe keys in LEFT join get null build side")
  void nullProbeKeysLeftJoin() {
    JoinHash hash = buildHash(new long[] {1}, new long[] {100});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.LEFT, new int[] {0}, new int[] {1}, new int[] {1});

    Page probe =
        probePageWithNulls(new long[] {1, 0}, new boolean[] {false, true}, new long[] {10, 20});
    op.addInput(probe);
    Page output = op.getOutput();

    assertNotNull(output);
    assertEquals(2, output.getPositionCount(), "LEFT join keeps all probe rows including null key");

    op.close();
  }

  @Test
  @DisplayName("Null build keys never match (null != null)")
  void nullBuildKeysNeverMatch() {
    JoinHash hash =
        buildHashWithNulls(new long[] {1, 0}, new boolean[] {false, true}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.INNER, new int[] {0}, new int[] {1}, new int[] {1});

    // Probe with null key — should NOT match null build key
    Page probe = probePageWithNulls(new long[] {0}, new boolean[] {true}, new long[] {10});
    op.addInput(probe);
    Page output = op.getOutput();

    assertNull(output, "null probe key should not match null build key");

    op.close();
  }

  // ==================== MULTI-KEY JOIN ====================

  @Test
  @DisplayName("Multi-column join key: matches on composite key")
  void multiColumnKeyJoin() {
    // Build with 2-column key
    HashBuilderOperator builder =
        new HashBuilderOperator(new OperatorContext(1, "HashBuilder"), new int[] {0, 1});
    builder.addInput(
        new Page(
            new LongArrayBlock(3, Optional.empty(), new long[] {1, 1, 2}),
            new LongArrayBlock(3, Optional.empty(), new long[] {10, 20, 10}),
            new LongArrayBlock(3, Optional.empty(), new long[] {100, 200, 300})));
    builder.finish();
    JoinHash hash = builder.getJoinHash();

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX,
            hash,
            JoinType.INNER,
            new int[] {0, 1}, // probe key on columns 0, 1
            new int[] {2}, // probe output: column 2 (value)
            new int[] {2}); // build output: column 2 (value)

    // Probe for (1, 20) which matches build row (1, 20, 200)
    Page probe =
        new Page(
            new LongArrayBlock(1, Optional.empty(), new long[] {1}),
            new LongArrayBlock(1, Optional.empty(), new long[] {20}),
            new LongArrayBlock(1, Optional.empty(), new long[] {999}));
    op.addInput(probe);
    Page output = op.getOutput();

    assertNotNull(output);
    assertEquals(1, output.getPositionCount());

    // Build output should be 200
    LongArrayBlock buildVal = (LongArrayBlock) output.getBlock(1);
    assertEquals(200L, buildVal.getLong(0));

    op.close();
  }

  // ==================== EMPTY SIDES ====================

  @Test
  @DisplayName("Empty build side: INNER join produces no output")
  void emptyBuildSideInnerJoin() {
    JoinHash hash = buildEmptyHash();

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.INNER, new int[] {0}, new int[] {1}, new int[] {});

    op.addInput(probePage(new long[] {1, 2}, new long[] {10, 20}));
    Page output = op.getOutput();

    assertNull(output, "Empty build side means no matches for INNER join");

    op.close();
  }

  @Test
  @DisplayName("Empty build side: LEFT join keeps all probe rows")
  void emptyBuildSideLeftJoin() {
    JoinHash hash = buildEmptyHash();

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.LEFT, new int[] {0}, new int[] {1}, new int[] {});

    op.addInput(probePage(new long[] {1, 2}, new long[] {10, 20}));
    Page output = op.getOutput();

    assertNotNull(output);
    assertEquals(2, output.getPositionCount(), "LEFT join keeps all probe rows");

    op.close();
  }

  @Test
  @DisplayName("Empty probe side: INNER join produces no output")
  void emptyProbeSideInnerJoin() {
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.INNER, new int[] {0}, new int[] {1}, new int[] {1});

    // No input added, just finish
    op.finish();
    assertTrue(op.isFinished());
    assertNull(op.getOutput());

    op.close();
  }

  @Test
  @DisplayName("Empty probe side: RIGHT join emits all build rows at finish")
  void emptyProbeSideRightJoin() {
    JoinHash hash = buildHash(new long[] {1, 2}, new long[] {100, 200});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.RIGHT, new int[] {0}, new int[] {1}, new int[] {1});

    // No probe input
    op.finish();
    assertFalse(op.isFinished());

    Page output = op.getOutput();
    assertTrue(op.isFinished());

    assertNotNull(output);
    assertEquals(2, output.getPositionCount(), "All build rows unmatched");

    op.close();
  }

  // ==================== LIFECYCLE ====================

  @Test
  @DisplayName("Operator lifecycle follows contract")
  void operatorLifecycle() {
    JoinHash hash = buildHash(new long[] {1}, new long[] {100});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.INNER, new int[] {0}, new int[] {1}, new int[] {1});

    assertTrue(op.needsInput());
    assertFalse(op.isFinished());
    assertTrue(op.isBlocked().isDone());
    assertNull(op.getOutput());

    op.addInput(probePage(new long[] {1}, new long[] {10}));
    assertFalse(op.needsInput(), "Should have output pending");

    Page output = op.getOutput();
    assertNotNull(output);
    assertTrue(op.needsInput(), "Ready for more input after output consumed");

    op.finish();
    assertTrue(op.isFinished());

    op.close();
  }

  @Test
  @DisplayName("addInput throws when not in NEEDS_INPUT state")
  void addInputThrowsWhenNotReady() {
    JoinHash hash = buildHash(new long[] {1}, new long[] {100});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.INNER, new int[] {0}, new int[] {1}, new int[] {1});

    op.addInput(probePage(new long[] {1}, new long[] {10}));

    // Now in HAS_OUTPUT state, should throw
    assertThrows(
        IllegalStateException.class, () -> op.addInput(probePage(new long[] {2}, new long[] {20})));

    op.close();
  }

  @Test
  @DisplayName("Empty probe page is skipped")
  void emptyProbePageSkipped() {
    JoinHash hash = buildHash(new long[] {1}, new long[] {100});

    LookupJoinOperator op =
        new LookupJoinOperator(
            CTX, hash, JoinType.INNER, new int[] {0}, new int[] {1}, new int[] {1});

    op.addInput(
        new Page(
            new LongArrayBlock(0, Optional.empty(), new long[0]),
            new LongArrayBlock(0, Optional.empty(), new long[0])));

    // Should still be in NEEDS_INPUT
    assertTrue(op.needsInput());

    op.close();
  }
}
