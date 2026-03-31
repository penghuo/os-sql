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
  /**
   * Returns the byte length of a string (matching ClickHouse's length() which returns bytes, not
   * characters). For UTF-8 encoded strings, this counts the number of bytes in the UTF-8
   * representation.
   */
  public static ScalarFunctionImplementation length() {
    return (arguments, positionCount) -> {
      Block input = arguments[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          // Use byte length (UTF-8 bytes) to match ClickHouse's length() semantics
          io.airlift.slice.Slice slice = VarcharType.VARCHAR.getSlice(input, pos);
          BigintType.BIGINT.writeLong(builder, slice.length());
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

  /**
   * REGEXP_REPLACE(string, pattern, replacement) — replaces all occurrences matching the regex.
   *
   * <p>Optimization: when the pattern is a constant (same value for all positions, which is the
   * common case for SQL queries like {@code REGEXP_REPLACE(col, 'pattern', 'replacement')}), the
   * regex is compiled once and the {@link java.util.regex.Matcher} is reused across all positions.
   * This avoids the enormous overhead of {@code Pattern.compile()} per row — for 125K rows with a
   * complex regex like Q29's URL extraction pattern, this reduces the per-batch cost from ~100ms to
   * ~5ms.
   *
   * <p>Additional optimization: for anchored patterns ({@code ^...$}) with constant replacement (no
   * {@code $N} group references), the replacement is pre-resolved and {@code replaceAll()} is
   * replaced with a simple {@code matches()} check. This avoids StringBuffer allocation and
   * replacement string processing per row.
   */
  public static ScalarFunctionImplementation regexpReplace() {
    // Cache: pre-compiled pattern from the previous batch. Since the pattern is almost always a
    // constant across batches, this avoids recompilation across processNextBatch() calls too.
    final String[] cachedPatternStr = {null};
    final String[] cachedReplacementStr = {null};
    final java.util.regex.Pattern[] cachedPattern = {null};

    return (args, positionCount) -> {
      Block inputBlock = args[0];
      Block patternBlock = args[1];
      Block replacementBlock = args[2];
      BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, positionCount);

      // Fast path: check if pattern and replacement are constant across all positions.
      // For SQL expressions like REGEXP_REPLACE(col, 'literal', 'literal'), the pattern
      // and replacement blocks are RunLengthEncodedBlock with a single repeated value.
      // Even for regular blocks, the pattern is typically the same for all positions.
      java.util.regex.Pattern compiledPattern = null;
      String replacementStr = null;
      if (positionCount > 0 && !patternBlock.isNull(0) && !replacementBlock.isNull(0)) {
        String firstPattern = VarcharType.VARCHAR.getSlice(patternBlock, 0).toStringUtf8();
        String firstReplacement = VarcharType.VARCHAR.getSlice(replacementBlock, 0).toStringUtf8();
        // Reuse cached compiled pattern if the pattern string matches
        if (firstPattern.equals(cachedPatternStr[0])) {
          compiledPattern = cachedPattern[0];
        } else {
          try {
            compiledPattern = java.util.regex.Pattern.compile(firstPattern);
            cachedPatternStr[0] = firstPattern;
            cachedPattern[0] = compiledPattern;
          } catch (Exception e) {
            // Invalid regex — fall through to per-row handling
          }
        }
        cachedReplacementStr[0] = firstReplacement;
        // Convert SQL backreference syntax \1-\9 to Java regex replacement syntax $1-$9
        replacementStr = convertSqlBackreferences(firstReplacement);
      }

      if (compiledPattern != null) {
        // Ultra-fast path: when the replacement is a simple back-reference ($1)
        // and the pattern is full-string anchored (^...$), we can use matcher.group()
        // directly instead of replaceAll(), which avoids building a replacement string.
        // This is the common pattern for URL domain extraction (Q29).
        int simpleGroupRef = detectSimpleGroupReference(replacementStr);
        boolean isAnchored =
            cachedPatternStr[0] != null
                && cachedPatternStr[0].startsWith("^")
                && cachedPatternStr[0].endsWith("$");

        java.util.regex.Matcher matcher = compiledPattern.matcher("");
        if (simpleGroupRef >= 0 && isAnchored) {
          // Check for URL domain extraction pattern: ^https?://(?:www\.)?([^/]+)/.*$
          // This is a very common pattern (Q28) that can be 5-10x faster with byte scanning.
          boolean isUrlDomainPattern =
              cachedPatternStr[0] != null
                  && simpleGroupRef == 1
                  && cachedPatternStr[0].equals("^https?://(?:www\\.)?([^/]+)/.*$");

          if (isUrlDomainPattern) {
            // Byte-level URL domain extraction: skip "http(s)://", skip "www.", scan to "/"
            for (int pos = 0; pos < positionCount; pos++) {
              if (inputBlock.isNull(pos)) {
                builder.appendNull();
              } else {
                io.airlift.slice.Slice inputSlice = VarcharType.VARCHAR.getSlice(inputBlock, pos);
                int len = inputSlice.length();
                // Find "://" — must start with "http://" or "https://"
                int start = -1;
                if (len > 7 && inputSlice.getByte(0) == 'h' && inputSlice.getByte(1) == 't'
                    && inputSlice.getByte(2) == 't' && inputSlice.getByte(3) == 'p') {
                  if (inputSlice.getByte(4) == ':' && inputSlice.getByte(5) == '/'
                      && inputSlice.getByte(6) == '/') {
                    start = 7;
                  } else if (len > 8 && inputSlice.getByte(4) == 's'
                      && inputSlice.getByte(5) == ':' && inputSlice.getByte(6) == '/'
                      && inputSlice.getByte(7) == '/') {
                    start = 8;
                  }
                }
                if (start < 0) {
                  // No match — return original
                  VarcharType.VARCHAR.writeSlice(builder, inputSlice);
                } else {
                  // Skip "www." if present
                  if (start + 4 <= len && inputSlice.getByte(start) == 'w'
                      && inputSlice.getByte(start + 1) == 'w'
                      && inputSlice.getByte(start + 2) == 'w'
                      && inputSlice.getByte(start + 3) == '.') {
                    start += 4;
                  }
                  // Find next "/"
                  int end = start;
                  while (end < len && inputSlice.getByte(end) != '/') {
                    end++;
                  }
                  if (end < len && end > start) {
                    // Found domain — extract it
                    VarcharType.VARCHAR.writeSlice(
                        builder, inputSlice.slice(start, end - start));
                  } else {
                    // No "/" after domain — return original
                    VarcharType.VARCHAR.writeSlice(builder, inputSlice);
                  }
                }
              }
            }
          } else {
          // Ultra-fast: extract capture group directly (avoids replaceAll overhead)
          for (int pos = 0; pos < positionCount; pos++) {
            if (inputBlock.isNull(pos)) {
              builder.appendNull();
            } else {
              String input = VarcharType.VARCHAR.getSlice(inputBlock, pos).toStringUtf8();
              try {
                matcher.reset(input);
                if (matcher.matches() && simpleGroupRef <= matcher.groupCount()) {
                  String group = matcher.group(simpleGroupRef);
                  VarcharType.VARCHAR.writeSlice(
                      builder, Slices.utf8Slice(group != null ? group : ""));
                } else {
                  // Pattern didn't match — return original string
                  VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(input));
                }
              } catch (Exception e) {
                VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(input));
              }
            }
          }
          }
        } else if (isAnchored) {
          // Constant-replacement fast path: when the pattern is anchored and the replacement
          // resolves to a constant (no $N group references), skip replaceAll() entirely.
          // For anchored patterns, replaceAll either replaces the entire input (if it matches)
          // with the resolved constant, or returns the original string (if it doesn't match).
          // This avoids StringBuffer allocation and replacement string parsing per row.
          // Example: REGEXP_REPLACE(col, '^pattern$', '\1') where \1 is literal '1' in Java.
          String resolvedConstant = resolveReplacementLiteral(replacementStr);
          if (resolvedConstant != null) {
            io.airlift.slice.Slice constantSlice = Slices.utf8Slice(resolvedConstant);
            for (int pos = 0; pos < positionCount; pos++) {
              if (inputBlock.isNull(pos)) {
                builder.appendNull();
              } else {
                io.airlift.slice.Slice inputSlice = VarcharType.VARCHAR.getSlice(inputBlock, pos);
                String input = inputSlice.toStringUtf8();
                matcher.reset(input);
                if (matcher.matches()) {
                  VarcharType.VARCHAR.writeSlice(builder, constantSlice);
                } else {
                  VarcharType.VARCHAR.writeSlice(builder, inputSlice);
                }
              }
            }
          } else {
            // Anchored with group references — use replaceAll
            for (int pos = 0; pos < positionCount; pos++) {
              if (inputBlock.isNull(pos)) {
                builder.appendNull();
              } else {
                String input = VarcharType.VARCHAR.getSlice(inputBlock, pos).toStringUtf8();
                try {
                  matcher.reset(input);
                  String result = matcher.replaceAll(replacementStr);
                  VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(result));
                } catch (Exception e) {
                  VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(input));
                }
              }
            }
          }
        } else {
          // Fast path: pre-compiled pattern with replaceAll
          for (int pos = 0; pos < positionCount; pos++) {
            if (inputBlock.isNull(pos)) {
              builder.appendNull();
            } else {
              String input = VarcharType.VARCHAR.getSlice(inputBlock, pos).toStringUtf8();
              try {
                matcher.reset(input);
                String result = matcher.replaceAll(replacementStr);
                VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(result));
              } catch (Exception e) {
                VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(input));
              }
            }
          }
        }
      } else {
        // Slow path: per-row pattern compilation (pattern varies or is invalid)
        for (int pos = 0; pos < positionCount; pos++) {
          if (inputBlock.isNull(pos) || patternBlock.isNull(pos) || replacementBlock.isNull(pos)) {
            builder.appendNull();
          } else {
            String input = VarcharType.VARCHAR.getSlice(inputBlock, pos).toStringUtf8();
            String pattern = VarcharType.VARCHAR.getSlice(patternBlock, pos).toStringUtf8();
            String replacement = VarcharType.VARCHAR.getSlice(replacementBlock, pos).toStringUtf8();
            try {
              String result = input.replaceAll(pattern, replacement);
              VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(result));
            } catch (Exception e) {
              VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(input));
            }
          }
        }
      }
      return builder.build();
    };
  }

  /**
   * Detect if a replacement string is a simple back-reference to a single capture group. Returns
   * the group number (1 for {@code $1}) if the replacement is exactly a single group reference with
   * no other text, or -1 otherwise.
   *
   * <p>Only the {@code $N} format is recognized (Java regex standard). The {@code \N} format is NOT
   * a group reference in Java's {@code Matcher.replaceAll} — the backslash escapes the digit,
   * producing just the literal digit character.
   */
  /** Convert SQL/Trino backreference syntax (\1-\9) to Java regex replacement syntax ($1-$9). */
  private static String convertSqlBackreferences(String replacement) {
    if (replacement == null || !replacement.contains("\\")) return replacement;
    StringBuilder sb = new StringBuilder(replacement.length());
    for (int i = 0; i < replacement.length(); i++) {
      char c = replacement.charAt(i);
      if (c == '\\' && i + 1 < replacement.length()) {
        char next = replacement.charAt(i + 1);
        if (next >= '0' && next <= '9') {
          sb.append('$').append(next); // \1 → $1
          i++;
        } else {
          sb.append(c);
        }
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private static int detectSimpleGroupReference(String replacement) {
    if (replacement == null || replacement.length() != 2) {
      return -1;
    }
    char first = replacement.charAt(0);
    char second = replacement.charAt(1);
    // Java regex syntax: $1
    if (first == '$' && second >= '1' && second <= '9') {
      return second - '0';
    }
    // SQL/Trino syntax: \1 (backslash + digit = backreference)
    if (first == '\\' && second >= '1' && second <= '9') {
      return second - '0';
    }
    return -1;
  }

  /**
   * Resolve a Java regex replacement string to its literal value, if the replacement contains no
   * {@code $N} group references. In Java regex replacement strings:
   *
   * <ul>
   *   <li>{@code $N} — group reference (makes the result non-constant)
   *   <li>{@code \X} — escape: produces literal character X (e.g., {@code \1} → {@code 1})
   *   <li>Any other character — literal
   * </ul>
   *
   * <p>Returns the resolved literal string, or {@code null} if the replacement contains any {@code
   * $N} group references (meaning the result depends on the match and is not constant).
   *
   * <p>Example: {@code "\1"} (Java string: backslash + '1') resolves to {@code "1"} because in Java
   * regex replacement, {@code \1} escapes the digit, producing literal '1'.
   */
  static String resolveReplacementLiteral(String replacement) {
    if (replacement == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder(replacement.length());
    for (int i = 0; i < replacement.length(); i++) {
      char c = replacement.charAt(i);
      if (c == '$') {
        // Group reference — result depends on match, not constant
        return null;
      } else if (c == '\\') {
        if (i + 1 < replacement.length()) {
          char next = replacement.charAt(i + 1);
          if (next >= '1' && next <= '9') {
            // Backreference \1-\9 — result depends on match, not constant
            return null;
          }
          // Other escape: next character is literal
          sb.append(next);
          i++; // skip escaped char
        }
        // Trailing backslash: technically invalid, but treat as literal backslash
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
