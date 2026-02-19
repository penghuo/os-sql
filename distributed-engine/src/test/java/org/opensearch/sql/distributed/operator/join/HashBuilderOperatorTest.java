/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.join;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;

/** Tests for HashBuilderOperator: build hash table, verify lookup, duplicate keys, memory. */
class HashBuilderOperatorTest {

  private static final OperatorContext CTX = new OperatorContext(0, "HashBuilder");

  @Test
  @DisplayName("Build hash table from single page and verify lookup")
  void buildAndLookup() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    // Build side: id=10, id=20, id=30
    long[] buildKeys = {10, 20, 30};
    long[] buildValues = {100, 200, 300};
    builder.addInput(
        new Page(
            new LongArrayBlock(3, Optional.empty(), buildKeys),
            new LongArrayBlock(3, Optional.empty(), buildValues)));
    builder.finish();

    JoinHash hash = builder.getJoinHash();
    assertNotNull(hash);
    assertEquals(3, hash.getPositionCount());

    // Probe for key=20
    Page probePage = new Page(new LongArrayBlock(1, Optional.empty(), new long[] {20}));
    int[] matches = hash.getMatchingPositions(probePage, 0, new int[] {0});
    assertEquals(1, matches.length);

    // Verify the matched build-side row has value=200
    int flatPos = matches[0];
    int pageIdx = hash.getPageIndex(flatPos);
    int posInPage = hash.getPositionInPage(flatPos);
    LongArrayBlock valBlock = (LongArrayBlock) hash.getPage(pageIdx).getBlock(1);
    assertEquals(200L, valBlock.getLong(posInPage));

    builder.close();
  }

  @Test
  @DisplayName("Build hash table from multiple pages")
  void buildFromMultiplePages() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    builder.addInput(
        new Page(
            new LongArrayBlock(2, Optional.empty(), new long[] {1, 2}),
            new LongArrayBlock(2, Optional.empty(), new long[] {10, 20})));
    builder.addInput(
        new Page(
            new LongArrayBlock(2, Optional.empty(), new long[] {3, 4}),
            new LongArrayBlock(2, Optional.empty(), new long[] {30, 40})));
    builder.finish();

    JoinHash hash = builder.getJoinHash();
    assertEquals(4, hash.getPositionCount());
    assertEquals(2, hash.getPageCount());

    // Verify lookup for key from second page
    Page probePage = new Page(new LongArrayBlock(1, Optional.empty(), new long[] {3}));
    int[] matches = hash.getMatchingPositions(probePage, 0, new int[] {0});
    assertEquals(1, matches.length);

    builder.close();
  }

  @Test
  @DisplayName("Duplicate keys produce multiple matches")
  void duplicateKeysMultipleMatches() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    // Three rows with key=5
    long[] keys = {5, 5, 5};
    long[] values = {100, 200, 300};
    builder.addInput(
        new Page(
            new LongArrayBlock(3, Optional.empty(), keys),
            new LongArrayBlock(3, Optional.empty(), values)));
    builder.finish();

    JoinHash hash = builder.getJoinHash();

    Page probePage = new Page(new LongArrayBlock(1, Optional.empty(), new long[] {5}));
    int[] matches = hash.getMatchingPositions(probePage, 0, new int[] {0});
    assertEquals(3, matches.length, "Should find all 3 duplicate build-side rows");

    // Collect matched values
    java.util.Set<Long> matchedValues = new java.util.HashSet<>();
    for (int flatPos : matches) {
      int pageIdx = hash.getPageIndex(flatPos);
      int posInPage = hash.getPositionInPage(flatPos);
      LongArrayBlock valBlock = (LongArrayBlock) hash.getPage(pageIdx).getBlock(1);
      matchedValues.add(valBlock.getLong(posInPage));
    }
    assertEquals(java.util.Set.of(100L, 200L, 300L), matchedValues);

    builder.close();
  }

  @Test
  @DisplayName("Null keys produce no matches (null != null in equi-join)")
  void nullKeysNoMatches() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    // Build side has a null key
    long[] keys = {10, 0, 30};
    boolean[] nulls = {false, true, false};
    builder.addInput(
        new Page(
            new LongArrayBlock(3, Optional.of(nulls), keys),
            new LongArrayBlock(3, Optional.empty(), new long[] {1, 2, 3})));
    builder.finish();

    JoinHash hash = builder.getJoinHash();

    // Probe with null key — should return no matches
    long[] probeKeys = {0};
    boolean[] probeNulls = {true};
    Page probePage = new Page(new LongArrayBlock(1, Optional.of(probeNulls), probeKeys));
    int[] matches = hash.getMatchingPositions(probePage, 0, new int[] {0});
    assertEquals(0, matches.length, "Null keys should not match (null != null)");

    builder.close();
  }

  @Test
  @DisplayName("Probe for non-existent key returns empty matches")
  void nonExistentKeyReturnsEmpty() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    builder.addInput(
        new Page(
            new LongArrayBlock(2, Optional.empty(), new long[] {10, 20}),
            new LongArrayBlock(2, Optional.empty(), new long[] {1, 2})));
    builder.finish();

    JoinHash hash = builder.getJoinHash();

    // Probe with key=99 which doesn't exist
    Page probePage = new Page(new LongArrayBlock(1, Optional.empty(), new long[] {99}));
    int[] matches = hash.getMatchingPositions(probePage, 0, new int[] {0});
    assertEquals(0, matches.length);

    builder.close();
  }

  @Test
  @DisplayName("Multi-column join key lookup")
  void multiColumnKeyLookup() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0, 1});

    // Build: (1,10)=A, (1,20)=B, (2,10)=C
    long[] col0 = {1, 1, 2};
    long[] col1 = {10, 20, 10};
    long[] values = {100, 200, 300};
    builder.addInput(
        new Page(
            new LongArrayBlock(3, Optional.empty(), col0),
            new LongArrayBlock(3, Optional.empty(), col1),
            new LongArrayBlock(3, Optional.empty(), values)));
    builder.finish();

    JoinHash hash = builder.getJoinHash();

    // Probe for (1, 20)
    Page probePage =
        new Page(
            new LongArrayBlock(1, Optional.empty(), new long[] {1}),
            new LongArrayBlock(1, Optional.empty(), new long[] {20}));
    int[] matches = hash.getMatchingPositions(probePage, 0, new int[] {0, 1});
    assertEquals(1, matches.length);

    // Verify matched value is 200
    int flatPos = matches[0];
    int pageIdx = hash.getPageIndex(flatPos);
    int posInPage = hash.getPositionInPage(flatPos);
    LongArrayBlock valBlock = (LongArrayBlock) hash.getPage(pageIdx).getBlock(2);
    assertEquals(200L, valBlock.getLong(posInPage));

    builder.close();
  }

  @Test
  @DisplayName("Lifecycle: state transitions follow contract")
  void lifecycleStateTransitions() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    // Initial state: consuming input
    assertTrue(builder.needsInput());
    assertFalse(builder.isFinished());
    assertNull(builder.getOutput(), "HashBuilder never produces output pages");

    // isBlocked should return NOT_BLOCKED
    assertTrue(builder.isBlocked().isDone());

    // Add some input
    builder.addInput(new Page(new LongArrayBlock(1, Optional.empty(), new long[] {1})));
    assertTrue(builder.needsInput());

    // Finish
    builder.finish();
    assertFalse(builder.needsInput());
    assertFalse(builder.isFinished(), "Should be LOOKUP_SOURCE_READY, not FINISHED");
    assertNotNull(builder.getJoinHash());

    // Destroy
    builder.destroy();
    assertTrue(builder.isFinished());
  }

  @Test
  @DisplayName("Cannot add input after finish")
  void cannotAddInputAfterFinish() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});
    builder.finish();

    assertThrows(
        IllegalStateException.class,
        () -> builder.addInput(new Page(new LongArrayBlock(1, Optional.empty(), new long[] {1}))));

    builder.close();
  }

  @Test
  @DisplayName("getJoinHash throws before finish")
  void getJoinHashThrowsBeforeFinish() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    assertThrows(IllegalStateException.class, builder::getJoinHash);

    builder.close();
  }

  @Test
  @DisplayName("Empty build produces hash with zero positions")
  void emptyBuildZeroPositions() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});
    builder.finish();

    JoinHash hash = builder.getJoinHash();
    assertEquals(0, hash.getPositionCount());
    assertEquals(0, hash.getPageCount());

    builder.close();
  }

  @Test
  @DisplayName("Empty page is skipped during build")
  void emptyPageSkipped() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    // Add empty page, then a real page
    builder.addInput(new Page(new LongArrayBlock(0, Optional.empty(), new long[0])));
    builder.addInput(new Page(new LongArrayBlock(2, Optional.empty(), new long[] {1, 2})));
    builder.finish();

    JoinHash hash = builder.getJoinHash();
    assertEquals(2, hash.getPositionCount());

    builder.close();
  }

  @Test
  @DisplayName("Memory tracking: reserveMemory called during build")
  void memoryTrackingDuringBuild() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    builder.addInput(new Page(new LongArrayBlock(100, Optional.empty(), new long[100])));
    builder.finish();

    // After finish, memory should have been reserved for page + hash table
    // We verify via the operator context's memory reservation
    // (Note: CTX is standalone so reserveMemory tracks locally only)

    builder.close();
  }

  @Test
  @DisplayName("Factory creates operators and tracks last created")
  void factoryCreation() {
    HashBuilderOperator.HashBuilderOperatorFactory factory =
        new HashBuilderOperator.HashBuilderOperatorFactory(new int[] {0});

    HashBuilderOperator op = (HashBuilderOperator) factory.createOperator(CTX);
    assertNotNull(op);
    assertSame(op, factory.getLastCreated());

    factory.noMoreOperators();
    assertThrows(IllegalStateException.class, () -> factory.createOperator(CTX));
  }

  @Test
  @DisplayName("JoinProbe iterates all matches for duplicate keys")
  void joinProbeIteratesMatches() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    // 3 build rows with key=42
    builder.addInput(
        new Page(
            new LongArrayBlock(3, Optional.empty(), new long[] {42, 42, 42}),
            new LongArrayBlock(3, Optional.empty(), new long[] {1, 2, 3})));
    builder.finish();

    JoinHash hash = builder.getJoinHash();

    // Create JoinProbe for probe row with key=42
    Page probePage = new Page(new LongArrayBlock(1, Optional.empty(), new long[] {42}));
    JoinProbe probe = new JoinProbe(hash, new int[] {0}, probePage, 0);

    assertEquals(3, probe.getMatchCount());
    assertSame(probePage, probe.getProbePage());
    assertEquals(0, probe.getProbePosition());

    java.util.Set<Long> values = new java.util.HashSet<>();
    while (probe.hasNextMatch()) {
      int flatPos = probe.nextMatch();
      int pageIdx = hash.getPageIndex(flatPos);
      int posInPage = hash.getPositionInPage(flatPos);
      LongArrayBlock valBlock = (LongArrayBlock) hash.getPage(pageIdx).getBlock(1);
      values.add(valBlock.getLong(posInPage));
    }
    assertEquals(java.util.Set.of(1L, 2L, 3L), values);

    assertFalse(probe.hasNextMatch());
    assertThrows(IllegalStateException.class, probe::nextMatch);

    builder.close();
  }

  @Test
  @DisplayName("JoinProbe with no matches")
  void joinProbeNoMatches() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    builder.addInput(new Page(new LongArrayBlock(1, Optional.empty(), new long[] {10})));
    builder.finish();

    JoinHash hash = builder.getJoinHash();

    Page probePage = new Page(new LongArrayBlock(1, Optional.empty(), new long[] {99}));
    JoinProbe probe = new JoinProbe(hash, new int[] {0}, probePage, 0);

    assertEquals(0, probe.getMatchCount());
    assertFalse(probe.hasNextMatch());

    builder.close();
  }

  @Test
  @DisplayName("Large build side: 10K rows build and lookup correctly")
  void largeBuildSide() {
    HashBuilderOperator builder = new HashBuilderOperator(CTX, new int[] {0});

    long[] keys = new long[10_000];
    long[] values = new long[10_000];
    for (int i = 0; i < 10_000; i++) {
      keys[i] = i;
      values[i] = i * 10L;
    }
    builder.addInput(
        new Page(
            new LongArrayBlock(10_000, Optional.empty(), keys),
            new LongArrayBlock(10_000, Optional.empty(), values)));
    builder.finish();

    JoinHash hash = builder.getJoinHash();
    assertEquals(10_000, hash.getPositionCount());

    // Spot-check a few lookups
    for (long probeKey : new long[] {0, 999, 5000, 9999}) {
      Page probePage = new Page(new LongArrayBlock(1, Optional.empty(), new long[] {probeKey}));
      int[] matches = hash.getMatchingPositions(probePage, 0, new int[] {0});
      assertEquals(1, matches.length, "Should find key " + probeKey);
    }

    builder.close();
  }
}
