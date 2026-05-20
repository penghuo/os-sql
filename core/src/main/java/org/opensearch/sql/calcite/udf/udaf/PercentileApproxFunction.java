/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import com.tdunning.math.stats.MergingDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

/** We write by ourselves since it's an approximate algorithm */
public class PercentileApproxFunction
    implements UserDefinedAggFunction<PercentileApproxFunction.PencentileApproAccumulator> {
  SqlTypeName returnType;
  private double compression;
  double percentile;

  @Override
  public PencentileApproAccumulator init() {
    returnType = SqlTypeName.DOUBLE;
    compression = 100.0;
    percentile = 1.0;
    return new PencentileApproAccumulator();
  }

  // Add values to the accumulator
  @Override
  public PencentileApproAccumulator add(PencentileApproAccumulator acc, Object... values) {
    Object targetValue = values[0];
    if (Objects.isNull(targetValue)) {
      return acc;
    }
    percentile = ((Number) values[1]).intValue() / 100.0;
    // The optional trailing flag is the SqlTypeName of the field, used to coerce the result
    // back to the field's declared type. The SqlNode emit always includes it; the older v2
    // path also includes it. When absent (or wrongly typed), infer from the target value's
    // runtime class so we don't blow up at the cast site.
    Object lastArg = values[values.length - 1];
    if (lastArg instanceof SqlTypeName s) {
      returnType = s;
    } else if (targetValue instanceof Long) {
      returnType = SqlTypeName.BIGINT;
    } else if (targetValue instanceof Integer) {
      returnType = SqlTypeName.INTEGER;
    } else if (targetValue instanceof Float) {
      returnType = SqlTypeName.FLOAT;
    } else {
      returnType = SqlTypeName.DOUBLE;
    }
    if (values.length > 3 && values[values.length - 2] instanceof Number compNum) {
      compression = compNum.doubleValue();
    }

    acc.evaluate(((Number) targetValue).doubleValue());
    return acc;
  }

  // Calculate the percentile
  @Override
  public Object result(PencentileApproAccumulator acc) {
    if (acc.size() == 0) {
      return null;
    }
    double retValue = (double) acc.value(compression, percentile);
    switch (returnType) {
      case INTEGER:
        int intRet = (int) retValue;
        return intRet;
      case BIGINT:
        long longRet = (long) retValue;
        return longRet;
      case FLOAT:
        float floatRet = (float) retValue;
        return floatRet;
      default:
        return retValue;
    }
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
