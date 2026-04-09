/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.Optional;
import java.util.regex.Pattern;

public class LikeBlockExpression implements BlockExpression {

  private final BlockExpression child;
  private final Pattern compiledPattern;
  private final MatchStrategy strategy;
  private final String literal;

  private enum MatchStrategy {
    CONTAINS, STARTS_WITH, ENDS_WITH, EQUALS, REGEX
  }

  public LikeBlockExpression(BlockExpression child, String likePattern) {
    this.child = child;
    if (!likePattern.contains("%") && !likePattern.contains("_")) {
      this.strategy = MatchStrategy.EQUALS;
      this.literal = likePattern;
      this.compiledPattern = null;
    } else if (likePattern.startsWith("%") && likePattern.endsWith("%")
        && likePattern.length() > 2
        && !likePattern.substring(1, likePattern.length() - 1).contains("%")
        && !likePattern.substring(1, likePattern.length() - 1).contains("_")) {
      this.strategy = MatchStrategy.CONTAINS;
      this.literal = likePattern.substring(1, likePattern.length() - 1);
      this.compiledPattern = null;
    } else if (!likePattern.startsWith("%") && likePattern.endsWith("%")
        && likePattern.indexOf('%') == likePattern.length() - 1
        && !likePattern.contains("_")) {
      this.strategy = MatchStrategy.STARTS_WITH;
      this.literal = likePattern.substring(0, likePattern.length() - 1);
      this.compiledPattern = null;
    } else if (likePattern.startsWith("%") && !likePattern.substring(1).contains("%")
        && !likePattern.contains("_")) {
      this.strategy = MatchStrategy.ENDS_WITH;
      this.literal = likePattern.substring(1);
      this.compiledPattern = null;
    } else {
      this.strategy = MatchStrategy.REGEX;
      this.literal = null;
      this.compiledPattern = compilePattern(likePattern);
    }
  }

  @Override
  public Block evaluate(Page page) {
    Block input = child.evaluate(page);
    int positionCount = page.getPositionCount();
    byte[] values = new byte[positionCount];
    if (!input.mayHaveNull()) {
      for (int pos = 0; pos < positionCount; pos++) {
        values[pos] = (byte) (matches(input, pos) ? 1 : 0);
      }
      return new ByteArrayBlock(positionCount, Optional.empty(), values);
    }
    boolean[] nulls = new boolean[positionCount];
    boolean hasNulls = false;
    for (int pos = 0; pos < positionCount; pos++) {
      if (input.isNull(pos)) {
        nulls[pos] = true;
        hasNulls = true;
      } else {
        values[pos] = (byte) (matches(input, pos) ? 1 : 0);
      }
    }
    return new ByteArrayBlock(
        positionCount, hasNulls ? Optional.of(nulls) : Optional.empty(), values);
  }

  private boolean matches(Block input, int pos) {
    String val = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
    return switch (strategy) {
      case CONTAINS -> val.contains(literal);
      case STARTS_WITH -> val.startsWith(literal);
      case ENDS_WITH -> val.endsWith(literal);
      case EQUALS -> val.equals(literal);
      case REGEX -> compiledPattern.matcher(val).matches();
    };
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }

  static Pattern compilePattern(String likePattern) {
    StringBuilder regex = new StringBuilder("^");
    for (int i = 0; i < likePattern.length(); i++) {
      char c = likePattern.charAt(i);
      if (c == '%') regex.append(".*");
      else if (c == '_') regex.append('.');
      else if (".^$+{[]|()\\" .indexOf(c) >= 0) regex.append('\\').append(c);
      else regex.append(c);
    }
    regex.append("$");
    return Pattern.compile(regex.toString(), Pattern.DOTALL);
  }
}
