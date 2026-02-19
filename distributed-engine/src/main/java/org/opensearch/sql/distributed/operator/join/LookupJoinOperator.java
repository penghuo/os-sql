/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.join;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BooleanArrayBlock;
import org.opensearch.sql.distributed.data.ByteArrayBlock;
import org.opensearch.sql.distributed.data.DictionaryBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.RunLengthEncodedBlock;
import org.opensearch.sql.distributed.data.ShortArrayBlock;
import org.opensearch.sql.distributed.data.VariableWidthBlock;
import org.opensearch.sql.distributed.operator.Operator;

/**
 * Probe-side join operator. For each probe-side row, looks up matching build-side rows from the
 * {@link JoinHash} and produces output pages combining probe and build columns.
 *
 * <p>Supports INNER, LEFT, RIGHT, FULL, SEMI, and ANTI join types.
 *
 * <p>Ported from Trino's io.trino.operator.join.LookupJoinOperator (simplified: no spill, no work
 * processor, single-threaded).
 */
public class LookupJoinOperator implements Operator {

  private enum State {
    NEEDS_INPUT,
    HAS_OUTPUT,
    FINISHING,
    FINISHED
  }

  private final OperatorContext operatorContext;
  private final JoinHash joinHash;
  private final JoinType joinType;
  private final int[] probeKeyChannels;
  private final int[] probeOutputChannels;
  private final int[] buildOutputChannels;

  private State state;
  private Page outputPage;

  /**
   * Tracks which build-side positions have been matched (for RIGHT/FULL joins). Only allocated when
   * needed.
   */
  private boolean[] buildMatched;

  /**
   * Creates a new LookupJoinOperator.
   *
   * @param operatorContext the operator context
   * @param joinHash the build-side hash table
   * @param joinType the join type
   * @param probeKeyChannels column indices in probe pages used as join keys
   * @param probeOutputChannels probe-side columns to include in output
   * @param buildOutputChannels build-side columns to include in output
   */
  public LookupJoinOperator(
      OperatorContext operatorContext,
      JoinHash joinHash,
      JoinType joinType,
      int[] probeKeyChannels,
      int[] probeOutputChannels,
      int[] buildOutputChannels) {
    this.operatorContext = Objects.requireNonNull(operatorContext, "operatorContext is null");
    this.joinHash = Objects.requireNonNull(joinHash, "joinHash is null");
    this.joinType = Objects.requireNonNull(joinType, "joinType is null");
    this.probeKeyChannels = Objects.requireNonNull(probeKeyChannels, "probeKeyChannels is null");
    this.probeOutputChannels =
        Objects.requireNonNull(probeOutputChannels, "probeOutputChannels is null");
    this.buildOutputChannels =
        Objects.requireNonNull(buildOutputChannels, "buildOutputChannels is null");
    this.state = State.NEEDS_INPUT;

    // For RIGHT/FULL joins, track which build rows were matched
    if (joinType.outputUnmatchedBuild()) {
      this.buildMatched = new boolean[joinHash.getPositionCount()];
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  @Override
  public boolean needsInput() {
    return state == State.NEEDS_INPUT;
  }

  @Override
  public void addInput(Page page) {
    if (state != State.NEEDS_INPUT) {
      throw new IllegalStateException("Operator does not need input, state=" + state);
    }
    Objects.requireNonNull(page, "page is null");
    if (page.getPositionCount() == 0) {
      return;
    }

    operatorContext.recordInputPositions(page.getPositionCount());
    outputPage = processProbe(page);
    if (outputPage != null && outputPage.getPositionCount() > 0) {
      state = State.HAS_OUTPUT;
    }
  }

  @Override
  public Page getOutput() {
    if (state == State.HAS_OUTPUT) {
      Page result = outputPage;
      outputPage = null;
      state = State.NEEDS_INPUT;
      if (result != null) {
        operatorContext.recordOutputPositions(result.getPositionCount());
      }
      return result;
    }
    if (state == State.FINISHING) {
      Page result = emitUnmatchedBuildRows();
      state = State.FINISHED;
      if (result != null) {
        operatorContext.recordOutputPositions(result.getPositionCount());
      }
      return result;
    }
    return null;
  }

  @Override
  public void finish() {
    if (state == State.NEEDS_INPUT) {
      if (joinType.outputUnmatchedBuild()) {
        state = State.FINISHING;
      } else {
        state = State.FINISHED;
      }
    }
  }

  @Override
  public boolean isFinished() {
    return state == State.FINISHED;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() {
    state = State.FINISHED;
    outputPage = null;
  }

  /** Processes a probe page against the build-side hash table and produces an output page. */
  private Page processProbe(Page probePage) {
    int probePositions = probePage.getPositionCount();

    // Collect output row builders
    List<int[]> probeIndices = new ArrayList<>();
    List<int[]> buildIndices = new ArrayList<>(); // -1 means null (unmatched)

    for (int probePos = 0; probePos < probePositions; probePos++) {
      JoinProbe probe = new JoinProbe(joinHash, probeKeyChannels, probePage, probePos);
      int matchCount = probe.getMatchCount();

      switch (joinType) {
        case INNER:
          while (probe.hasNextMatch()) {
            int buildPos = probe.nextMatch();
            probeIndices.add(new int[] {probePos});
            buildIndices.add(new int[] {buildPos});
          }
          break;

        case LEFT:
          if (matchCount == 0) {
            probeIndices.add(new int[] {probePos});
            buildIndices.add(new int[] {-1}); // null build side
          } else {
            while (probe.hasNextMatch()) {
              int buildPos = probe.nextMatch();
              probeIndices.add(new int[] {probePos});
              buildIndices.add(new int[] {buildPos});
            }
          }
          break;

        case RIGHT:
          while (probe.hasNextMatch()) {
            int buildPos = probe.nextMatch();
            buildMatched[buildPos] = true;
            probeIndices.add(new int[] {probePos});
            buildIndices.add(new int[] {buildPos});
          }
          break;

        case FULL:
          if (matchCount == 0) {
            probeIndices.add(new int[] {probePos});
            buildIndices.add(new int[] {-1});
          } else {
            while (probe.hasNextMatch()) {
              int buildPos = probe.nextMatch();
              buildMatched[buildPos] = true;
              probeIndices.add(new int[] {probePos});
              buildIndices.add(new int[] {buildPos});
            }
          }
          break;

        case SEMI:
          if (matchCount > 0) {
            probeIndices.add(new int[] {probePos});
            buildIndices.add(null); // no build columns
          }
          break;

        case ANTI:
          if (matchCount == 0) {
            probeIndices.add(new int[] {probePos});
            buildIndices.add(null); // no build columns
          }
          break;
      }
    }

    if (probeIndices.isEmpty()) {
      return null;
    }

    return buildOutputPage(probePage, probeIndices, buildIndices);
  }

  /**
   * Builds an output page from collected probe and build position indices.
   *
   * @param probePage the probe-side page
   * @param probeIndices list of single-element arrays, each containing the probe position
   * @param buildIndices list of single-element arrays, each containing the build flat position (-1
   *     for null/unmatched), or null for SEMI/ANTI
   */
  private Page buildOutputPage(Page probePage, List<int[]> probeIndices, List<int[]> buildIndices) {
    int outputPositions = probeIndices.size();

    // Flatten probe positions
    int[] probePositionArray = new int[outputPositions];
    for (int i = 0; i < outputPositions; i++) {
      probePositionArray[i] = probeIndices.get(i)[0];
    }

    // Build probe-side output blocks
    Block[] probeBlocks = new Block[probeOutputChannels.length];
    for (int col = 0; col < probeOutputChannels.length; col++) {
      Block sourceBlock = probePage.getBlock(probeOutputChannels[col]);
      probeBlocks[col] = copyPositionsFromBlock(sourceBlock, probePositionArray, outputPositions);
    }

    if (!joinType.outputBuildColumns() || buildOutputChannels.length == 0) {
      return new Page(outputPositions, probeBlocks);
    }

    // Flatten build positions
    int[] buildPositionArray = new int[outputPositions];
    boolean[] buildNullMask = new boolean[outputPositions];
    boolean hasBuildNull = false;
    for (int i = 0; i < outputPositions; i++) {
      int[] bp = buildIndices.get(i);
      if (bp == null || bp[0] < 0) {
        buildPositionArray[i] = -1;
        buildNullMask[i] = true;
        hasBuildNull = true;
      } else {
        buildPositionArray[i] = bp[0];
      }
    }

    // Build build-side output blocks
    Block[] buildBlocks = new Block[buildOutputChannels.length];
    for (int col = 0; col < buildOutputChannels.length; col++) {
      buildBlocks[col] =
          buildBuildSideBlock(
              buildOutputChannels[col],
              buildPositionArray,
              buildNullMask,
              hasBuildNull,
              outputPositions);
    }

    // Combine probe + build blocks
    Block[] allBlocks = new Block[probeBlocks.length + buildBlocks.length];
    System.arraycopy(probeBlocks, 0, allBlocks, 0, probeBlocks.length);
    System.arraycopy(buildBlocks, 0, allBlocks, probeBlocks.length, buildBlocks.length);

    return new Page(outputPositions, allBlocks);
  }

  /**
   * Builds output blocks for the unmatched build-side rows (RIGHT/FULL joins). Called at finish.
   */
  private Page emitUnmatchedBuildRows() {
    if (buildMatched == null) {
      return null;
    }

    // Count unmatched build rows
    int unmatchedCount = 0;
    for (int i = 0; i < buildMatched.length; i++) {
      if (!buildMatched[i]) {
        unmatchedCount++;
      }
    }
    if (unmatchedCount == 0) {
      return null;
    }

    // Collect unmatched build positions
    int[] unmatchedPositions = new int[unmatchedCount];
    int idx = 0;
    for (int i = 0; i < buildMatched.length; i++) {
      if (!buildMatched[i]) {
        unmatchedPositions[idx++] = i;
      }
    }

    // Build null probe-side blocks
    Block[] probeBlocks = new Block[probeOutputChannels.length];
    for (int col = 0; col < probeOutputChannels.length; col++) {
      probeBlocks[col] = createNullBlock(unmatchedCount);
    }

    // Build build-side blocks from unmatched positions
    boolean[] noBuildNulls = new boolean[unmatchedCount]; // all false
    Block[] buildBlocks = new Block[buildOutputChannels.length];
    for (int col = 0; col < buildOutputChannels.length; col++) {
      buildBlocks[col] =
          buildBuildSideBlock(
              buildOutputChannels[col], unmatchedPositions, noBuildNulls, false, unmatchedCount);
    }

    Block[] allBlocks = new Block[probeBlocks.length + buildBlocks.length];
    System.arraycopy(probeBlocks, 0, allBlocks, 0, probeBlocks.length);
    System.arraycopy(buildBlocks, 0, allBlocks, probeBlocks.length, buildBlocks.length);

    return new Page(unmatchedCount, allBlocks);
  }

  /**
   * Builds a build-side output block by gathering values from the JoinHash at the given flat
   * positions.
   */
  private Block buildBuildSideBlock(
      int buildChannel, int[] flatPositions, boolean[] nullMask, boolean hasNull, int outputCount) {
    // Determine block type from build-side sample
    if (joinHash.getPositionCount() == 0) {
      // No build-side data at all — return null block
      return createNullBlock(outputCount);
    }

    // Find a non-null position to determine block type
    Block sampleBlock = null;
    for (int i = 0; i < outputCount; i++) {
      if (!nullMask[i]) {
        int flatPos = flatPositions[i];
        int pageIdx = joinHash.getPageIndex(flatPos);
        int posInPage = joinHash.getPositionInPage(flatPos);
        Block block = joinHash.getPage(pageIdx).getBlock(buildChannel);
        sampleBlock = resolveValueBlock(block);
        break;
      }
    }

    if (sampleBlock == null) {
      // All positions are null
      return createNullBlock(outputCount);
    }

    if (sampleBlock instanceof LongArrayBlock) {
      return buildLongBlock(buildChannel, flatPositions, nullMask, hasNull, outputCount);
    }
    if (sampleBlock instanceof DoubleArrayBlock) {
      return buildDoubleBlock(buildChannel, flatPositions, nullMask, hasNull, outputCount);
    }
    if (sampleBlock instanceof IntArrayBlock) {
      return buildIntBlock(buildChannel, flatPositions, nullMask, hasNull, outputCount);
    }
    if (sampleBlock instanceof ShortArrayBlock) {
      return buildShortBlock(buildChannel, flatPositions, nullMask, hasNull, outputCount);
    }
    if (sampleBlock instanceof ByteArrayBlock) {
      return buildByteBlock(buildChannel, flatPositions, nullMask, hasNull, outputCount);
    }
    if (sampleBlock instanceof BooleanArrayBlock) {
      return buildBooleanBlock(buildChannel, flatPositions, nullMask, hasNull, outputCount);
    }
    if (sampleBlock instanceof VariableWidthBlock) {
      return buildVariableWidthBlock(buildChannel, flatPositions, nullMask, hasNull, outputCount);
    }
    throw new UnsupportedOperationException(
        "Unsupported block type: " + sampleBlock.getClass().getSimpleName());
  }

  private LongArrayBlock buildLongBlock(
      int channel, int[] flatPositions, boolean[] nullMask, boolean hasNull, int count) {
    long[] values = new long[count];
    boolean[] nulls = hasNull ? Arrays.copyOf(nullMask, count) : null;
    for (int i = 0; i < count; i++) {
      if (nullMask[i]) continue;
      int flatPos = flatPositions[i];
      int pageIdx = joinHash.getPageIndex(flatPos);
      int posInPage = joinHash.getPositionInPage(flatPos);
      Block block = joinHash.getPage(pageIdx).getBlock(channel);
      if (block.isNull(posInPage)) {
        if (nulls == null) nulls = new boolean[count];
        nulls[i] = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, posInPage);
        values[i] = ((LongArrayBlock) resolved).getLong(resolvedPos);
      }
    }
    return new LongArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  private DoubleArrayBlock buildDoubleBlock(
      int channel, int[] flatPositions, boolean[] nullMask, boolean hasNull, int count) {
    double[] values = new double[count];
    boolean[] nulls = hasNull ? Arrays.copyOf(nullMask, count) : null;
    for (int i = 0; i < count; i++) {
      if (nullMask[i]) continue;
      int flatPos = flatPositions[i];
      int pageIdx = joinHash.getPageIndex(flatPos);
      int posInPage = joinHash.getPositionInPage(flatPos);
      Block block = joinHash.getPage(pageIdx).getBlock(channel);
      if (block.isNull(posInPage)) {
        if (nulls == null) nulls = new boolean[count];
        nulls[i] = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, posInPage);
        values[i] = ((DoubleArrayBlock) resolved).getDouble(resolvedPos);
      }
    }
    return new DoubleArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  private IntArrayBlock buildIntBlock(
      int channel, int[] flatPositions, boolean[] nullMask, boolean hasNull, int count) {
    int[] values = new int[count];
    boolean[] nulls = hasNull ? Arrays.copyOf(nullMask, count) : null;
    for (int i = 0; i < count; i++) {
      if (nullMask[i]) continue;
      int flatPos = flatPositions[i];
      int pageIdx = joinHash.getPageIndex(flatPos);
      int posInPage = joinHash.getPositionInPage(flatPos);
      Block block = joinHash.getPage(pageIdx).getBlock(channel);
      if (block.isNull(posInPage)) {
        if (nulls == null) nulls = new boolean[count];
        nulls[i] = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, posInPage);
        values[i] = ((IntArrayBlock) resolved).getInt(resolvedPos);
      }
    }
    return new IntArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  private ShortArrayBlock buildShortBlock(
      int channel, int[] flatPositions, boolean[] nullMask, boolean hasNull, int count) {
    short[] values = new short[count];
    boolean[] nulls = hasNull ? Arrays.copyOf(nullMask, count) : null;
    for (int i = 0; i < count; i++) {
      if (nullMask[i]) continue;
      int flatPos = flatPositions[i];
      int pageIdx = joinHash.getPageIndex(flatPos);
      int posInPage = joinHash.getPositionInPage(flatPos);
      Block block = joinHash.getPage(pageIdx).getBlock(channel);
      if (block.isNull(posInPage)) {
        if (nulls == null) nulls = new boolean[count];
        nulls[i] = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, posInPage);
        values[i] = ((ShortArrayBlock) resolved).getShort(resolvedPos);
      }
    }
    return new ShortArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  private ByteArrayBlock buildByteBlock(
      int channel, int[] flatPositions, boolean[] nullMask, boolean hasNull, int count) {
    byte[] values = new byte[count];
    boolean[] nulls = hasNull ? Arrays.copyOf(nullMask, count) : null;
    for (int i = 0; i < count; i++) {
      if (nullMask[i]) continue;
      int flatPos = flatPositions[i];
      int pageIdx = joinHash.getPageIndex(flatPos);
      int posInPage = joinHash.getPositionInPage(flatPos);
      Block block = joinHash.getPage(pageIdx).getBlock(channel);
      if (block.isNull(posInPage)) {
        if (nulls == null) nulls = new boolean[count];
        nulls[i] = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, posInPage);
        values[i] = ((ByteArrayBlock) resolved).getByte(resolvedPos);
      }
    }
    return new ByteArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  private BooleanArrayBlock buildBooleanBlock(
      int channel, int[] flatPositions, boolean[] nullMask, boolean hasNull, int count) {
    boolean[] values = new boolean[count];
    boolean[] nulls = hasNull ? Arrays.copyOf(nullMask, count) : null;
    for (int i = 0; i < count; i++) {
      if (nullMask[i]) continue;
      int flatPos = flatPositions[i];
      int pageIdx = joinHash.getPageIndex(flatPos);
      int posInPage = joinHash.getPositionInPage(flatPos);
      Block block = joinHash.getPage(pageIdx).getBlock(channel);
      if (block.isNull(posInPage)) {
        if (nulls == null) nulls = new boolean[count];
        nulls[i] = true;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, posInPage);
        values[i] = ((BooleanArrayBlock) resolved).getBoolean(resolvedPos);
      }
    }
    return new BooleanArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  private VariableWidthBlock buildVariableWidthBlock(
      int channel, int[] flatPositions, boolean[] nullMask, boolean hasNull, int count) {
    // First pass: collect all byte data
    byte[][] slices = new byte[count][];
    boolean[] nulls = hasNull ? Arrays.copyOf(nullMask, count) : null;
    int totalBytes = 0;
    for (int i = 0; i < count; i++) {
      if (nullMask[i]) {
        slices[i] = EMPTY_BYTES;
        continue;
      }
      int flatPos = flatPositions[i];
      int pageIdx = joinHash.getPageIndex(flatPos);
      int posInPage = joinHash.getPositionInPage(flatPos);
      Block block = joinHash.getPage(pageIdx).getBlock(channel);
      if (block.isNull(posInPage)) {
        if (nulls == null) nulls = new boolean[count];
        nulls[i] = true;
        slices[i] = EMPTY_BYTES;
      } else {
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, posInPage);
        slices[i] = ((VariableWidthBlock) resolved).getSlice(resolvedPos);
        totalBytes += slices[i].length;
      }
    }

    // Build contiguous byte array
    byte[] data = new byte[totalBytes];
    int[] offsets = new int[count + 1];
    int byteOffset = 0;
    for (int i = 0; i < count; i++) {
      System.arraycopy(slices[i], 0, data, byteOffset, slices[i].length);
      offsets[i] = byteOffset;
      byteOffset += slices[i].length;
    }
    offsets[count] = byteOffset;
    return new VariableWidthBlock(count, data, offsets, Optional.ofNullable(nulls));
  }

  /** Creates a null-only LongArrayBlock as a default null block. */
  private static Block createNullBlock(int positionCount) {
    long[] values = new long[positionCount];
    boolean[] nulls = new boolean[positionCount];
    Arrays.fill(nulls, true);
    return new LongArrayBlock(positionCount, Optional.of(nulls), values);
  }

  /** Copies positions from a source block into a new block. Handles Dictionary and RLE wrappers. */
  private static Block copyPositionsFromBlock(Block source, int[] positions, int count) {
    Block resolved = resolveValueBlock(source);

    // Build resolved position array
    int[] resolvedPositions = new int[count];
    for (int i = 0; i < count; i++) {
      resolvedPositions[i] = resolvePosition(source, positions[i]);
    }

    if (resolved instanceof LongArrayBlock longBlock) {
      return longBlock.copyPositions(resolvedPositions, 0, count);
    }
    if (resolved instanceof DoubleArrayBlock doubleBlock) {
      return doubleBlock.copyPositions(resolvedPositions, 0, count);
    }
    if (resolved instanceof IntArrayBlock intBlock) {
      return intBlock.copyPositions(resolvedPositions, 0, count);
    }
    if (resolved instanceof ShortArrayBlock shortBlock) {
      return shortBlock.copyPositions(resolvedPositions, 0, count);
    }
    if (resolved instanceof ByteArrayBlock byteBlock) {
      return byteBlock.copyPositions(resolvedPositions, 0, count);
    }
    if (resolved instanceof BooleanArrayBlock boolBlock) {
      return boolBlock.copyPositions(resolvedPositions, 0, count);
    }
    if (resolved instanceof VariableWidthBlock varBlock) {
      return varBlock.copyPositions(resolvedPositions, 0, count);
    }
    throw new UnsupportedOperationException(
        "Unsupported block type: " + resolved.getClass().getSimpleName());
  }

  private static Block resolveValueBlock(Block block) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getDictionary();
    }
    if (block instanceof RunLengthEncodedBlock rle) {
      return rle.getValue();
    }
    return block;
  }

  private static int resolvePosition(Block block, int position) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getId(position);
    }
    if (block instanceof RunLengthEncodedBlock) {
      return 0;
    }
    return position;
  }

  private static final byte[] EMPTY_BYTES = new byte[0];
}
