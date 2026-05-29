/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import com.tdunning.math.stats.MergingDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/** We write by ourselves since it's an approximate algorithm */
public class PercentileApproxFunction
    implements UserDefinedAggFunction<PercentileApproxFunction.PencentileApproAccumulator> {
  private String returnTypeName;
  private double compression;
  double percentile;

  @Override
  public PencentileApproAccumulator init() {
    returnTypeName = "DOUBLE";
    compression = 100.0;
    percentile = 1.0;
    return new PencentileApproAccumulator();
  }

  // Add values to the accumulator. Operand layout (trailing String is the field's SqlTypeName
  // name, used to coerce the double result back to the declared return type):
  //   values[0]      = target value (the field being percentile-d)
  //   values[1]      = percentile (number, 0-100)
  //   values[2]      = optional compression (number) when present
  //   values[last]   = SqlTypeName.name() string ("BIGINT", "DOUBLE", "INTEGER", ...)
  @Override
  public PencentileApproAccumulator add(PencentileApproAccumulator acc, Object... values) {
    Object targetValue = values[0];
    if (Objects.isNull(targetValue)) {
      return acc;
    }
    percentile = ((Number) values[1]).intValue() / 100.0;
    Object trailing = values[values.length - 1];
    returnTypeName =
        (trailing instanceof String) ? (String) trailing : trailing.toString();
    if (values.length > 3) {
      compression = ((Number) values[values.length - 2]).doubleValue();
    }
    acc.evaluate(((Number) targetValue).doubleValue());
    return acc;
  }

  // Calculate the percentile and coerce to the declared return type. Calcite's UDAF codegen
  // emits a direct unboxing cast (e.g. `(Long) result`) so the runtime type must match the
  // declared SqlTypeName; otherwise we get ClassCastException at execution time.
  @Override
  public Object result(PencentileApproAccumulator acc) {
    if (acc.size() == 0) {
      return null;
    }
    double retValue = (double) acc.value(compression, percentile);
    return switch (returnTypeName) {
      case "TINYINT" -> (byte) retValue;
      case "SMALLINT" -> (short) retValue;
      case "INTEGER" -> (int) retValue;
      case "BIGINT" -> (long) retValue;
      case "REAL", "FLOAT" -> (float) retValue;
      default -> retValue;
    };
  }

  public static class PencentileApproAccumulator implements Accumulator {
    private List<Number> candidate;

    public int size() {
      return candidate.size();
    }

    public PencentileApproAccumulator() {
      candidate = new ArrayList<>();
    }

    public void evaluate(double value) {
      candidate.add(value);
    }

    @Override
    public Object value(Object... argList) {
      double percent = (double) argList[1];
      double compression = (double) argList[0];
      MergingDigest tree = new MergingDigest(compression);
      for (Number num : candidate) {
        tree.add(num.doubleValue());
      }
      return tree.quantile(percent);
    }
  }
}
