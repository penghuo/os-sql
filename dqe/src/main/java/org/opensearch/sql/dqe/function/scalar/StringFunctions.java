/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.scalar;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;

/**
 * Static factory methods that return vectorized {@link ScalarFunctionImplementation}s for
 * Trino-compatible string scalar functions.
 */
public final class StringFunctions {

  private StringFunctions() {}

  /** upper(varchar) -> varchar: converts every character to uppercase. */
  public static ScalarFunctionImplementation upper() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value.toUpperCase()));
        }
      }
      return builder.build();
    };
  }

  /** lower(varchar) -> varchar: converts every character to lowercase. */
  public static ScalarFunctionImplementation lower() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value.toLowerCase()));
        }
      }
      return builder.build();
    };
  }

  /** length(varchar) -> bigint: returns the character length of the string. */
  public static ScalarFunctionImplementation length() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          BigintType.BIGINT.writeLong(builder, value.length());
        }
      }
      return builder.build();
    };
  }

  /** concat(varchar, varchar) -> varchar: concatenates two strings. */
  public static ScalarFunctionImplementation concat() {
    return (arguments, positionCount) -> {
      Block left = arguments[0];
      Block right = arguments[1];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (left.isNull(pos) || right.isNull(pos)) {
          builder.appendNull();
        } else {
          String l = VarcharType.VARCHAR.getSlice(left, pos).toStringUtf8();
          String r = VarcharType.VARCHAR.getSlice(right, pos).toStringUtf8();
          VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(l + r));
        }
      }
      return builder.build();
    };
  }

  /**
   * substring(varchar, bigint, bigint) -> varchar: extracts a substring using 1-indexed start
   * position and a length.
   */
  public static ScalarFunctionImplementation substring() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      Block startBlock = arguments[1];
      Block lengthBlock = arguments[2];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos) || startBlock.isNull(pos) || lengthBlock.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          int start = (int) BigintType.BIGINT.getLong(startBlock, pos) - 1;
          int len = (int) BigintType.BIGINT.getLong(lengthBlock, pos);
          int end = Math.min(start + len, value.length());
          VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value.substring(start, end)));
        }
      }
      return builder.build();
    };
  }

  /** trim(varchar) -> varchar: removes leading and trailing whitespace. */
  public static ScalarFunctionImplementation trim() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value.trim()));
        }
      }
      return builder.build();
    };
  }

  /** ltrim(varchar) -> varchar: removes leading whitespace. */
  public static ScalarFunctionImplementation ltrim() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value.stripLeading()));
        }
      }
      return builder.build();
    };
  }

  /** rtrim(varchar) -> varchar: removes trailing whitespace. */
  public static ScalarFunctionImplementation rtrim() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value.stripTrailing()));
        }
      }
      return builder.build();
    };
  }

  /**
   * replace(varchar, varchar, varchar) -> varchar: replaces all occurrences of the search string
   * with the replacement string.
   */
  public static ScalarFunctionImplementation replace() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      Block search = arguments[1];
      Block replacement = arguments[2];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos) || search.isNull(pos) || replacement.isNull(pos)) {
          builder.appendNull();
        } else {
          String v = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          String s = VarcharType.VARCHAR.getSlice(search, pos).toStringUtf8();
          String r = VarcharType.VARCHAR.getSlice(replacement, pos).toStringUtf8();
          VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(v.replace(s, r)));
        }
      }
      return builder.build();
    };
  }

  /**
   * position(varchar, varchar) -> bigint: returns the 1-indexed position of the first occurrence of
   * the search string, or 0 if not found.
   */
  public static ScalarFunctionImplementation position() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      Block search = arguments[1];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos) || search.isNull(pos)) {
          builder.appendNull();
        } else {
          String v = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          String s = VarcharType.VARCHAR.getSlice(search, pos).toStringUtf8();
          int idx = v.indexOf(s);
          BigintType.BIGINT.writeLong(builder, idx >= 0 ? idx + 1 : 0);
        }
      }
      return builder.build();
    };
  }

  /** reverse(varchar) -> varchar: reverses the characters in the string. */
  public static ScalarFunctionImplementation reverse() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          VarcharType.VARCHAR.writeSlice(
              builder, Slices.utf8Slice(new StringBuilder(value).reverse().toString()));
        }
      }
      return builder.build();
    };
  }

  /**
   * lpad(varchar, bigint, varchar) -> varchar: left-pads the string to the target length using the
   * specified padding characters.
   */
  public static ScalarFunctionImplementation lpad() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      Block targetLengthBlock = arguments[1];
      Block padBlock = arguments[2];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos) || targetLengthBlock.isNull(pos) || padBlock.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          int targetLength = (int) BigintType.BIGINT.getLong(targetLengthBlock, pos);
          String pad = VarcharType.VARCHAR.getSlice(padBlock, pos).toStringUtf8();
          VarcharType.VARCHAR.writeSlice(
              builder, Slices.utf8Slice(leftPad(value, targetLength, pad)));
        }
      }
      return builder.build();
    };
  }

  /**
   * rpad(varchar, bigint, varchar) -> varchar: right-pads the string to the target length using the
   * specified padding characters.
   */
  public static ScalarFunctionImplementation rpad() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      Block targetLengthBlock = arguments[1];
      Block padBlock = arguments[2];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos) || targetLengthBlock.isNull(pos) || padBlock.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          int targetLength = (int) BigintType.BIGINT.getLong(targetLengthBlock, pos);
          String pad = VarcharType.VARCHAR.getSlice(padBlock, pos).toStringUtf8();
          VarcharType.VARCHAR.writeSlice(
              builder, Slices.utf8Slice(rightPad(value, targetLength, pad)));
        }
      }
      return builder.build();
    };
  }

  /**
   * starts_with(varchar, varchar) -> boolean: returns true if the string starts with the specified
   * prefix.
   */
  public static ScalarFunctionImplementation startsWith() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      Block prefix = arguments[1];
      BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos) || prefix.isNull(pos)) {
          builder.appendNull();
        } else {
          String v = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          String p = VarcharType.VARCHAR.getSlice(prefix, pos).toStringUtf8();
          BooleanType.BOOLEAN.writeBoolean(builder, v.startsWith(p));
        }
      }
      return builder.build();
    };
  }

  /** chr(bigint) -> varchar: returns the character for the given Unicode code point. */
  public static ScalarFunctionImplementation chr() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          long codePoint = BigintType.BIGINT.getLong(input, pos);
          VarcharType.VARCHAR.writeSlice(
              builder, Slices.utf8Slice(String.valueOf((char) codePoint)));
        }
      }
      return builder.build();
    };
  }

  /** codepoint(varchar) -> bigint: returns the Unicode code point of the first character. */
  public static ScalarFunctionImplementation codepoint() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          String value = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
          BigintType.BIGINT.writeLong(builder, value.codePointAt(0));
        }
      }
      return builder.build();
    };
  }

  private static String leftPad(String value, int targetLength, String pad) {
    if (value.length() >= targetLength) {
      return value.substring(0, targetLength);
    }
    StringBuilder sb = new StringBuilder();
    int charsNeeded = targetLength - value.length();
    while (sb.length() < charsNeeded) {
      sb.append(pad);
    }
    return sb.substring(0, charsNeeded) + value;
  }

  private static String rightPad(String value, int targetLength, String pad) {
    if (value.length() >= targetLength) {
      return value.substring(0, targetLength);
    }
    StringBuilder sb = new StringBuilder(value);
    while (sb.length() < targetLength) {
      sb.append(pad);
    }
    return sb.substring(0, targetLength);
  }

  /** REGEXP_REPLACE(string, pattern, replacement) — replaces all occurrences matching the regex. */
  public static ScalarFunctionImplementation regexpReplace() {
    return (args, positionCount) -> {
      Block inputBlock = args[0];
      Block patternBlock = args[1];
      Block replacementBlock = args[2];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (inputBlock.isNull(pos) || patternBlock.isNull(pos) || replacementBlock.isNull(pos)) {
          builder.appendNull();
        } else {
          String input = VarcharType.VARCHAR.getSlice(inputBlock, pos).toStringUtf8();
          String pattern = VarcharType.VARCHAR.getSlice(patternBlock, pos).toStringUtf8();
          String replacement = VarcharType.VARCHAR.getSlice(replacementBlock, pos).toStringUtf8();
          try {
            String result = input.replaceAll(pattern, replacement);
            VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(result));
          } catch (Exception e) {
            // If regex fails, return original string
            VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(input));
          }
        }
      }
      return builder.build();
    };
  }
}
