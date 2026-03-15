/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.List;

/**
 * Vectorized CASE WHEN expression. Evaluates WHEN conditions in order and picks the first matching
 * THEN result. Falls back to ELSE (or null).
 */
@lombok.Getter
public class CaseBlockExpression implements BlockExpression {

  private final List<BlockExpression> whenConditions;
  private final List<BlockExpression> thenResults;
  private final BlockExpression elseResult; // nullable
  private final Type outputType;

  public CaseBlockExpression(
      List<BlockExpression> whenConditions,
      List<BlockExpression> thenResults,
      BlockExpression elseResult,
      Type outputType) {
    this.whenConditions = whenConditions;
    this.thenResults = thenResults;
    this.elseResult = elseResult;
    this.outputType = outputType;
  }

  @Override
  public Block evaluate(Page page) {
    int positionCount = page.getPositionCount();
    // Pre-evaluate all conditions and results
    Block[] condBlocks = whenConditions.stream().map(c -> c.evaluate(page)).toArray(Block[]::new);
    Block[] resultBlocks = thenResults.stream().map(r -> r.evaluate(page)).toArray(Block[]::new);
    Block elseBlock = elseResult != null ? elseResult.evaluate(page) : null;

    BlockBuilder builder = outputType.createBlockBuilder(null, positionCount);

    for (int pos = 0; pos < positionCount; pos++) {
      boolean matched = false;
      for (int i = 0; i < condBlocks.length; i++) {
        if (!condBlocks[i].isNull(pos) && BooleanType.BOOLEAN.getBoolean(condBlocks[i], pos)) {
          copyValue(resultBlocks[i], pos, builder);
          matched = true;
          break;
        }
      }
      if (!matched) {
        if (elseBlock != null) {
          copyValue(elseBlock, pos, builder);
        } else {
          builder.appendNull();
        }
      }
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return outputType;
  }

  private void copyValue(Block source, int pos, BlockBuilder builder) {
    if (source.isNull(pos)) {
      builder.appendNull();
    } else if (outputType instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, VarcharType.VARCHAR.getSlice(source, pos));
    } else if (outputType instanceof BigintType) {
      BigintType.BIGINT.writeLong(builder, BigintType.BIGINT.getLong(source, pos));
    } else if (outputType instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, DoubleType.DOUBLE.getDouble(source, pos));
    } else if (outputType instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, BooleanType.BOOLEAN.getBoolean(source, pos));
    } else {
      throw new UnsupportedOperationException("Unsupported CASE output type: " + outputType);
    }
  }
}
