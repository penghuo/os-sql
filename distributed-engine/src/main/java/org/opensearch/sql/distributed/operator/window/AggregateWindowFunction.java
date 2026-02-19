/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.window;

import java.util.Optional;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;

/**
 * Aggregate window function that computes SUM, AVG, MIN, or MAX over a window frame. Supports both
 * ROWS and RANGE frame types.
 *
 * <p>For the initial implementation, this supports UNBOUNDED PRECEDING to CURRENT ROW (running
 * aggregates) and UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING (full partition aggregates).
 */
public class AggregateWindowFunction implements WindowFunction {

  /** The type of aggregate to compute. */
  public enum AggregateType {
    SUM,
    AVG,
    MIN,
    MAX,
    COUNT
  }

  /** The window frame type. */
  public enum FrameType {
    /** Frame from the start of partition to the current row. */
    ROWS_UNBOUNDED_PRECEDING_TO_CURRENT_ROW,
    /** Frame covering the entire partition. */
    ROWS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING
  }

  private final AggregateType aggregateType;
  private final int valueChannel;
  private final FrameType frameType;
  private final boolean outputDouble;

  /**
   * Creates an aggregate window function.
   *
   * @param aggregateType the aggregate type (SUM, AVG, MIN, MAX, COUNT)
   * @param valueChannel the column index to aggregate (-1 for COUNT(*))
   * @param frameType the window frame type
   * @param outputDouble true if the output should be DOUBLE type
   */
  public AggregateWindowFunction(
      AggregateType aggregateType, int valueChannel, FrameType frameType, boolean outputDouble) {
    this.aggregateType = aggregateType;
    this.valueChannel = valueChannel;
    this.frameType = frameType;
    this.outputDouble = outputDouble;
  }

  @Override
  public Block processPartition(
      PartitionData partition, int partitionSize, int[] orderKeyChannels) {
    switch (aggregateType) {
      case SUM:
        return computeSum(partition, partitionSize);
      case AVG:
        return computeAvg(partition, partitionSize);
      case MIN:
        return computeMin(partition, partitionSize);
      case MAX:
        return computeMax(partition, partitionSize);
      case COUNT:
        return computeCount(partition, partitionSize);
      default:
        throw new UnsupportedOperationException("Unsupported aggregate: " + aggregateType);
    }
  }

  private Block computeSum(PartitionData partition, int partitionSize) {
    if (frameType == FrameType.ROWS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING) {
      return computeFullPartitionSum(partition, partitionSize);
    }
    // Running sum
    if (outputDouble) {
      double[] values = new double[partitionSize];
      boolean[] nulls = new boolean[partitionSize];
      boolean hasNull = false;
      double runningSum = 0;
      boolean hasValue = false;
      for (int i = 0; i < partitionSize; i++) {
        if (!partition.isNull(i, valueChannel)) {
          runningSum += ((Number) partition.getValue(i, valueChannel)).doubleValue();
          hasValue = true;
        }
        if (hasValue) {
          values[i] = runningSum;
        } else {
          nulls[i] = true;
          hasNull = true;
        }
      }
      return new DoubleArrayBlock(
          partitionSize, hasNull ? Optional.of(nulls) : Optional.empty(), values);
    } else {
      long[] values = new long[partitionSize];
      boolean[] nulls = new boolean[partitionSize];
      boolean hasNull = false;
      long runningSum = 0;
      boolean hasValue = false;
      for (int i = 0; i < partitionSize; i++) {
        if (!partition.isNull(i, valueChannel)) {
          runningSum += ((Number) partition.getValue(i, valueChannel)).longValue();
          hasValue = true;
        }
        if (hasValue) {
          values[i] = runningSum;
        } else {
          nulls[i] = true;
          hasNull = true;
        }
      }
      return new LongArrayBlock(
          partitionSize, hasNull ? Optional.of(nulls) : Optional.empty(), values);
    }
  }

  private Block computeFullPartitionSum(PartitionData partition, int partitionSize) {
    if (outputDouble) {
      double total = 0;
      boolean hasValue = false;
      for (int i = 0; i < partitionSize; i++) {
        if (!partition.isNull(i, valueChannel)) {
          total += ((Number) partition.getValue(i, valueChannel)).doubleValue();
          hasValue = true;
        }
      }
      double[] values = new double[partitionSize];
      boolean[] nulls = null;
      if (!hasValue) {
        nulls = new boolean[partitionSize];
        java.util.Arrays.fill(nulls, true);
      } else {
        java.util.Arrays.fill(values, total);
      }
      return new DoubleArrayBlock(partitionSize, Optional.ofNullable(nulls), values);
    } else {
      long total = 0;
      boolean hasValue = false;
      for (int i = 0; i < partitionSize; i++) {
        if (!partition.isNull(i, valueChannel)) {
          total += ((Number) partition.getValue(i, valueChannel)).longValue();
          hasValue = true;
        }
      }
      long[] values = new long[partitionSize];
      boolean[] nulls = null;
      if (!hasValue) {
        nulls = new boolean[partitionSize];
        java.util.Arrays.fill(nulls, true);
      } else {
        java.util.Arrays.fill(values, total);
      }
      return new LongArrayBlock(partitionSize, Optional.ofNullable(nulls), values);
    }
  }

  private Block computeAvg(PartitionData partition, int partitionSize) {
    // AVG always outputs double
    double[] values = new double[partitionSize];
    boolean[] nulls = new boolean[partitionSize];
    boolean hasNull = false;

    if (frameType == FrameType.ROWS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING) {
      double total = 0;
      int count = 0;
      for (int i = 0; i < partitionSize; i++) {
        if (!partition.isNull(i, valueChannel)) {
          total += ((Number) partition.getValue(i, valueChannel)).doubleValue();
          count++;
        }
      }
      if (count == 0) {
        java.util.Arrays.fill(nulls, true);
        hasNull = true;
      } else {
        double avg = total / count;
        java.util.Arrays.fill(values, avg);
      }
    } else {
      // Running average
      double runningSum = 0;
      int runningCount = 0;
      for (int i = 0; i < partitionSize; i++) {
        if (!partition.isNull(i, valueChannel)) {
          runningSum += ((Number) partition.getValue(i, valueChannel)).doubleValue();
          runningCount++;
        }
        if (runningCount > 0) {
          values[i] = runningSum / runningCount;
        } else {
          nulls[i] = true;
          hasNull = true;
        }
      }
    }
    return new DoubleArrayBlock(
        partitionSize, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  private Block computeMin(PartitionData partition, int partitionSize) {
    if (outputDouble) {
      double[] values = new double[partitionSize];
      boolean[] nulls = new boolean[partitionSize];
      boolean hasNull = false;

      if (frameType == FrameType.ROWS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING) {
        double min = Double.MAX_VALUE;
        boolean found = false;
        for (int i = 0; i < partitionSize; i++) {
          if (!partition.isNull(i, valueChannel)) {
            double v = ((Number) partition.getValue(i, valueChannel)).doubleValue();
            if (!found || v < min) min = v;
            found = true;
          }
        }
        if (!found) {
          java.util.Arrays.fill(nulls, true);
          hasNull = true;
        } else {
          java.util.Arrays.fill(values, min);
        }
      } else {
        double runningMin = Double.MAX_VALUE;
        boolean found = false;
        for (int i = 0; i < partitionSize; i++) {
          if (!partition.isNull(i, valueChannel)) {
            double v = ((Number) partition.getValue(i, valueChannel)).doubleValue();
            if (!found || v < runningMin) runningMin = v;
            found = true;
          }
          if (found) {
            values[i] = runningMin;
          } else {
            nulls[i] = true;
            hasNull = true;
          }
        }
      }
      return new DoubleArrayBlock(
          partitionSize, hasNull ? Optional.of(nulls) : Optional.empty(), values);
    } else {
      long[] values = new long[partitionSize];
      boolean[] nulls = new boolean[partitionSize];
      boolean hasNull = false;

      if (frameType == FrameType.ROWS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING) {
        long min = Long.MAX_VALUE;
        boolean found = false;
        for (int i = 0; i < partitionSize; i++) {
          if (!partition.isNull(i, valueChannel)) {
            long v = ((Number) partition.getValue(i, valueChannel)).longValue();
            if (!found || v < min) min = v;
            found = true;
          }
        }
        if (!found) {
          java.util.Arrays.fill(nulls, true);
          hasNull = true;
        } else {
          java.util.Arrays.fill(values, min);
        }
      } else {
        long runningMin = Long.MAX_VALUE;
        boolean found = false;
        for (int i = 0; i < partitionSize; i++) {
          if (!partition.isNull(i, valueChannel)) {
            long v = ((Number) partition.getValue(i, valueChannel)).longValue();
            if (!found || v < runningMin) runningMin = v;
            found = true;
          }
          if (found) {
            values[i] = runningMin;
          } else {
            nulls[i] = true;
            hasNull = true;
          }
        }
      }
      return new LongArrayBlock(
          partitionSize, hasNull ? Optional.of(nulls) : Optional.empty(), values);
    }
  }

  private Block computeMax(PartitionData partition, int partitionSize) {
    if (outputDouble) {
      double[] values = new double[partitionSize];
      boolean[] nulls = new boolean[partitionSize];
      boolean hasNull = false;

      if (frameType == FrameType.ROWS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING) {
        double max = -Double.MAX_VALUE;
        boolean found = false;
        for (int i = 0; i < partitionSize; i++) {
          if (!partition.isNull(i, valueChannel)) {
            double v = ((Number) partition.getValue(i, valueChannel)).doubleValue();
            if (!found || v > max) max = v;
            found = true;
          }
        }
        if (!found) {
          java.util.Arrays.fill(nulls, true);
          hasNull = true;
        } else {
          java.util.Arrays.fill(values, max);
        }
      } else {
        double runningMax = -Double.MAX_VALUE;
        boolean found = false;
        for (int i = 0; i < partitionSize; i++) {
          if (!partition.isNull(i, valueChannel)) {
            double v = ((Number) partition.getValue(i, valueChannel)).doubleValue();
            if (!found || v > runningMax) runningMax = v;
            found = true;
          }
          if (found) {
            values[i] = runningMax;
          } else {
            nulls[i] = true;
            hasNull = true;
          }
        }
      }
      return new DoubleArrayBlock(
          partitionSize, hasNull ? Optional.of(nulls) : Optional.empty(), values);
    } else {
      long[] values = new long[partitionSize];
      boolean[] nulls = new boolean[partitionSize];
      boolean hasNull = false;

      if (frameType == FrameType.ROWS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING) {
        long max = Long.MIN_VALUE;
        boolean found = false;
        for (int i = 0; i < partitionSize; i++) {
          if (!partition.isNull(i, valueChannel)) {
            long v = ((Number) partition.getValue(i, valueChannel)).longValue();
            if (!found || v > max) max = v;
            found = true;
          }
        }
        if (!found) {
          java.util.Arrays.fill(nulls, true);
          hasNull = true;
        } else {
          java.util.Arrays.fill(values, max);
        }
      } else {
        long runningMax = Long.MIN_VALUE;
        boolean found = false;
        for (int i = 0; i < partitionSize; i++) {
          if (!partition.isNull(i, valueChannel)) {
            long v = ((Number) partition.getValue(i, valueChannel)).longValue();
            if (!found || v > runningMax) runningMax = v;
            found = true;
          }
          if (found) {
            values[i] = runningMax;
          } else {
            nulls[i] = true;
            hasNull = true;
          }
        }
      }
      return new LongArrayBlock(
          partitionSize, hasNull ? Optional.of(nulls) : Optional.empty(), values);
    }
  }

  private Block computeCount(PartitionData partition, int partitionSize) {
    long[] values = new long[partitionSize];

    if (frameType == FrameType.ROWS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING) {
      long total = 0;
      for (int i = 0; i < partitionSize; i++) {
        if (valueChannel < 0 || !partition.isNull(i, valueChannel)) {
          total++;
        }
      }
      java.util.Arrays.fill(values, total);
    } else {
      long runningCount = 0;
      for (int i = 0; i < partitionSize; i++) {
        if (valueChannel < 0 || !partition.isNull(i, valueChannel)) {
          runningCount++;
        }
        values[i] = runningCount;
      }
    }
    return new LongArrayBlock(partitionSize, Optional.empty(), values);
  }

  @Override
  public String getName() {
    return aggregateType.name() + " OVER";
  }
}
