/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.regex.Pattern;

/**
 * Vectorized LIKE expression. Compiles the SQL LIKE pattern to a {@link Pattern} at construction
 * time for efficient repeated matching.
 */
public class LikeBlockExpression implements BlockExpression {

  private final BlockExpression child;
  private final Pattern compiledPattern;

  public LikeBlockExpression(BlockExpression child, String likePattern) {
    this.child = child;
    this.compiledPattern = compilePattern(likePattern);
  }

  @Override
  public Block evaluate(Page page) {
    Block input = child.evaluate(page);
    int positionCount = page.getPositionCount();
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, positionCount);

    for (int pos = 0; pos < positionCount; pos++) {
      if (input.isNull(pos)) {
        builder.appendNull();
      } else {
        String val = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
        BooleanType.BOOLEAN.writeBoolean(builder, compiledPattern.matcher(val).matches());
      }
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }

  /** Convert SQL LIKE pattern to Java regex. % -> .*, _ -> ., escape special chars. */
  static Pattern compilePattern(String likePattern) {
    StringBuilder regex = new StringBuilder("^");
    for (int i = 0; i < likePattern.length(); i++) {
      char c = likePattern.charAt(i);
      if (c == '%') {
        regex.append(".*");
      } else if (c == '_') {
        regex.append('.');
      } else if (".^$+{[]|()\\".indexOf(c) >= 0) {
        regex.append('\\').append(c);
      } else {
        regex.append(c);
      }
    }
    regex.append("$");
    return Pattern.compile(regex.toString(), Pattern.DOTALL);
  }
}
