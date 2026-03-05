/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.scalar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("StringFunctions")
class StringFunctionsTest {

  // ---------------------------------------------------------------------------
  // upper
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("upper()")
  class Upper {

    @Test
    @DisplayName("converts strings to uppercase")
    void normalInput() {
      Block input = varcharBlock("hello", "World", "ALREADY");
      Block result = StringFunctions.upper().evaluate(new Block[] {input}, 3);

      assertVarchar("HELLO", result, 0);
      assertVarchar("WORLD", result, 1);
      assertVarchar("ALREADY", result, 2);
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull("hello", null, "world");
      Block result = StringFunctions.upper().evaluate(new Block[] {input}, 3);

      assertVarchar("HELLO", result, 0);
      assertTrue(result.isNull(1));
      assertVarchar("WORLD", result, 2);
    }

    @Test
    @DisplayName("handles empty string")
    void emptyString() {
      Block input = varcharBlock("");
      Block result = StringFunctions.upper().evaluate(new Block[] {input}, 1);
      assertVarchar("", result, 0);
    }
  }

  // ---------------------------------------------------------------------------
  // lower
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("lower()")
  class Lower {

    @Test
    @DisplayName("converts strings to lowercase")
    void normalInput() {
      Block input = varcharBlock("HELLO", "World", "already");
      Block result = StringFunctions.lower().evaluate(new Block[] {input}, 3);

      assertVarchar("hello", result, 0);
      assertVarchar("world", result, 1);
      assertVarchar("already", result, 2);
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull(null, "WORLD");
      Block result = StringFunctions.lower().evaluate(new Block[] {input}, 2);

      assertTrue(result.isNull(0));
      assertVarchar("world", result, 1);
    }
  }

  // ---------------------------------------------------------------------------
  // length
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("length()")
  class Length {

    @Test
    @DisplayName("returns string lengths")
    void normalInput() {
      Block input = varcharBlock("hello", "ab", "");
      Block result = StringFunctions.length().evaluate(new Block[] {input}, 3);

      assertEquals(5L, BigintType.BIGINT.getLong(result, 0));
      assertEquals(2L, BigintType.BIGINT.getLong(result, 1));
      assertEquals(0L, BigintType.BIGINT.getLong(result, 2));
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull("abc", null);
      Block result = StringFunctions.length().evaluate(new Block[] {input}, 2);

      assertEquals(3L, BigintType.BIGINT.getLong(result, 0));
      assertTrue(result.isNull(1));
    }
  }

  // ---------------------------------------------------------------------------
  // concat
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("concat()")
  class Concat {

    @Test
    @DisplayName("concatenates two strings")
    void normalInput() {
      Block left = varcharBlock("hello", "foo");
      Block right = varcharBlock(" world", "bar");
      Block result = StringFunctions.concat().evaluate(new Block[] {left, right}, 2);

      assertVarchar("hello world", result, 0);
      assertVarchar("foobar", result, 1);
    }

    @Test
    @DisplayName("propagates null from either argument")
    void nullInput() {
      Block left = varcharBlockWithNull("hello", null, "x");
      Block right = varcharBlockWithNull(" world", "y", null);
      Block result = StringFunctions.concat().evaluate(new Block[] {left, right}, 3);

      assertVarchar("hello world", result, 0);
      assertTrue(result.isNull(1));
      assertTrue(result.isNull(2));
    }

    @Test
    @DisplayName("handles empty strings")
    void emptyStrings() {
      Block left = varcharBlock("", "abc");
      Block right = varcharBlock("xyz", "");
      Block result = StringFunctions.concat().evaluate(new Block[] {left, right}, 2);

      assertVarchar("xyz", result, 0);
      assertVarchar("abc", result, 1);
    }
  }

  // ---------------------------------------------------------------------------
  // substring
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("substring()")
  class Substring {

    @Test
    @DisplayName("extracts substring with 1-indexed start")
    void normalInput() {
      Block input = varcharBlock("hello world", "abcdef");
      Block start = bigintBlock(1L, 3L);
      Block len = bigintBlock(5L, 2L);
      Block result = StringFunctions.substring().evaluate(new Block[] {input, start, len}, 2);

      assertVarchar("hello", result, 0);
      assertVarchar("cd", result, 1);
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull(null, "hello");
      Block start = bigintBlock(1L, 1L);
      Block len = bigintBlock(3L, 3L);
      Block result = StringFunctions.substring().evaluate(new Block[] {input, start, len}, 2);

      assertTrue(result.isNull(0));
      assertVarchar("hel", result, 1);
    }

    @Test
    @DisplayName("clamps length to string bounds")
    void lengthExceedsBounds() {
      Block input = varcharBlock("hi");
      Block start = bigintBlock(1L);
      Block len = bigintBlock(100L);
      Block result = StringFunctions.substring().evaluate(new Block[] {input, start, len}, 1);

      assertVarchar("hi", result, 0);
    }
  }

  // ---------------------------------------------------------------------------
  // trim, ltrim, rtrim
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("trim()")
  class Trim {

    @Test
    @DisplayName("removes leading and trailing whitespace")
    void normalInput() {
      Block input = varcharBlock("  hello  ", "world", "  spaced  ");
      Block result = StringFunctions.trim().evaluate(new Block[] {input}, 3);

      assertVarchar("hello", result, 0);
      assertVarchar("world", result, 1);
      assertVarchar("spaced", result, 2);
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull((String) null);
      Block result = StringFunctions.trim().evaluate(new Block[] {input}, 1);
      assertTrue(result.isNull(0));
    }
  }

  @Nested
  @DisplayName("ltrim()")
  class Ltrim {

    @Test
    @DisplayName("removes leading whitespace only")
    void normalInput() {
      Block input = varcharBlock("  hello  ");
      Block result = StringFunctions.ltrim().evaluate(new Block[] {input}, 1);
      assertVarchar("hello  ", result, 0);
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull((String) null);
      Block result = StringFunctions.ltrim().evaluate(new Block[] {input}, 1);
      assertTrue(result.isNull(0));
    }
  }

  @Nested
  @DisplayName("rtrim()")
  class Rtrim {

    @Test
    @DisplayName("removes trailing whitespace only")
    void normalInput() {
      Block input = varcharBlock("  hello  ");
      Block result = StringFunctions.rtrim().evaluate(new Block[] {input}, 1);
      assertVarchar("  hello", result, 0);
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull((String) null);
      Block result = StringFunctions.rtrim().evaluate(new Block[] {input}, 1);
      assertTrue(result.isNull(0));
    }
  }

  // ---------------------------------------------------------------------------
  // replace
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("replace()")
  class Replace {

    @Test
    @DisplayName("replaces all occurrences")
    void normalInput() {
      Block input = varcharBlock("hello world", "aabaa");
      Block search = varcharBlock("o", "a");
      Block replacement = varcharBlock("0", "x");
      Block result =
          StringFunctions.replace().evaluate(new Block[] {input, search, replacement}, 2);

      assertVarchar("hell0 w0rld", result, 0);
      assertVarchar("xxbxx", result, 1);
    }

    @Test
    @DisplayName("propagates null from any argument")
    void nullInput() {
      Block input = varcharBlockWithNull((String) null);
      Block search = varcharBlock("a");
      Block replacement = varcharBlock("b");
      Block result =
          StringFunctions.replace().evaluate(new Block[] {input, search, replacement}, 1);
      assertTrue(result.isNull(0));
    }
  }

  // ---------------------------------------------------------------------------
  // position
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("position()")
  class Position {

    @Test
    @DisplayName("returns 1-indexed position or 0 if not found")
    void normalInput() {
      Block input = varcharBlock("hello world", "abcdef", "miss");
      Block search = varcharBlock("world", "cd", "xyz");
      Block result = StringFunctions.position().evaluate(new Block[] {input, search}, 3);

      assertEquals(7L, BigintType.BIGINT.getLong(result, 0));
      assertEquals(3L, BigintType.BIGINT.getLong(result, 1));
      assertEquals(0L, BigintType.BIGINT.getLong(result, 2));
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull((String) null);
      Block search = varcharBlock("x");
      Block result = StringFunctions.position().evaluate(new Block[] {input, search}, 1);
      assertTrue(result.isNull(0));
    }
  }

  // ---------------------------------------------------------------------------
  // reverse
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("reverse()")
  class Reverse {

    @Test
    @DisplayName("reverses strings")
    void normalInput() {
      Block input = varcharBlock("hello", "abc", "");
      Block result = StringFunctions.reverse().evaluate(new Block[] {input}, 3);

      assertVarchar("olleh", result, 0);
      assertVarchar("cba", result, 1);
      assertVarchar("", result, 2);
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull((String) null);
      Block result = StringFunctions.reverse().evaluate(new Block[] {input}, 1);
      assertTrue(result.isNull(0));
    }
  }

  // ---------------------------------------------------------------------------
  // lpad
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("lpad()")
  class Lpad {

    @Test
    @DisplayName("left pads to target length")
    void normalInput() {
      Block input = varcharBlock("hi", "hello");
      Block targetLength = bigintBlock(5L, 10L);
      Block pad = varcharBlock("*", "-+");
      Block result = StringFunctions.lpad().evaluate(new Block[] {input, targetLength, pad}, 2);

      assertVarchar("***hi", result, 0);
      assertVarchar("-+-+-hello", result, 1);
    }

    @Test
    @DisplayName("truncates when string exceeds target length")
    void truncates() {
      Block input = varcharBlock("hello world");
      Block targetLength = bigintBlock(5L);
      Block pad = varcharBlock("*");
      Block result = StringFunctions.lpad().evaluate(new Block[] {input, targetLength, pad}, 1);

      assertVarchar("hello", result, 0);
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull((String) null);
      Block targetLength = bigintBlock(5L);
      Block pad = varcharBlock("*");
      Block result = StringFunctions.lpad().evaluate(new Block[] {input, targetLength, pad}, 1);
      assertTrue(result.isNull(0));
    }
  }

  // ---------------------------------------------------------------------------
  // rpad
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("rpad()")
  class Rpad {

    @Test
    @DisplayName("right pads to target length")
    void normalInput() {
      Block input = varcharBlock("hi", "hello");
      Block targetLength = bigintBlock(5L, 10L);
      Block pad = varcharBlock("*", "-+");
      Block result = StringFunctions.rpad().evaluate(new Block[] {input, targetLength, pad}, 2);

      assertVarchar("hi***", result, 0);
      assertVarchar("hello-+-+-", result, 1);
    }

    @Test
    @DisplayName("truncates when string exceeds target length")
    void truncates() {
      Block input = varcharBlock("hello world");
      Block targetLength = bigintBlock(5L);
      Block pad = varcharBlock("*");
      Block result = StringFunctions.rpad().evaluate(new Block[] {input, targetLength, pad}, 1);

      assertVarchar("hello", result, 0);
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull((String) null);
      Block targetLength = bigintBlock(5L);
      Block pad = varcharBlock("*");
      Block result = StringFunctions.rpad().evaluate(new Block[] {input, targetLength, pad}, 1);
      assertTrue(result.isNull(0));
    }
  }

  // ---------------------------------------------------------------------------
  // starts_with
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("starts_with()")
  class StartsWith {

    @Test
    @DisplayName("checks string prefix")
    void normalInput() {
      Block input = varcharBlock("hello world", "goodbye", "hello");
      Block prefix = varcharBlock("hello", "good", "world");
      Block result = StringFunctions.startsWith().evaluate(new Block[] {input, prefix}, 3);

      assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
      assertTrue(BooleanType.BOOLEAN.getBoolean(result, 1));
      assertFalse(BooleanType.BOOLEAN.getBoolean(result, 2));
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull(null, "hello");
      Block prefix = varcharBlock("x", "he");
      Block result = StringFunctions.startsWith().evaluate(new Block[] {input, prefix}, 2);

      assertTrue(result.isNull(0));
      assertTrue(BooleanType.BOOLEAN.getBoolean(result, 1));
    }

    @Test
    @DisplayName("empty prefix always matches")
    void emptyPrefix() {
      Block input = varcharBlock("anything");
      Block prefix = varcharBlock("");
      Block result = StringFunctions.startsWith().evaluate(new Block[] {input, prefix}, 1);
      assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    }
  }

  // ---------------------------------------------------------------------------
  // chr
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("chr()")
  class Chr {

    @Test
    @DisplayName("converts code points to characters")
    void normalInput() {
      Block input = bigintBlock(65L, 97L, 48L);
      Block result = StringFunctions.chr().evaluate(new Block[] {input}, 3);

      assertVarchar("A", result, 0);
      assertVarchar("a", result, 1);
      assertVarchar("0", result, 2);
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 2);
      BigintType.BIGINT.writeLong(builder, 65L);
      builder.appendNull();
      Block input = builder.build();
      Block result = StringFunctions.chr().evaluate(new Block[] {input}, 2);

      assertVarchar("A", result, 0);
      assertTrue(result.isNull(1));
    }
  }

  // ---------------------------------------------------------------------------
  // codepoint
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("codepoint()")
  class Codepoint {

    @Test
    @DisplayName("returns Unicode code point of first character")
    void normalInput() {
      Block input = varcharBlock("A", "a", "0");
      Block result = StringFunctions.codepoint().evaluate(new Block[] {input}, 3);

      assertEquals(65L, BigintType.BIGINT.getLong(result, 0));
      assertEquals(97L, BigintType.BIGINT.getLong(result, 1));
      assertEquals(48L, BigintType.BIGINT.getLong(result, 2));
    }

    @Test
    @DisplayName("propagates null")
    void nullInput() {
      Block input = varcharBlockWithNull((String) null);
      Block result = StringFunctions.codepoint().evaluate(new Block[] {input}, 1);
      assertTrue(result.isNull(0));
    }

    @Test
    @DisplayName("uses first character of multi-character string")
    void multiCharString() {
      Block input = varcharBlock("hello");
      Block result = StringFunctions.codepoint().evaluate(new Block[] {input}, 1);
      assertEquals((long) 'h', BigintType.BIGINT.getLong(result, 0));
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static Block varcharBlock(String... values) {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, values.length);
    for (String v : values) {
      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(v));
    }
    return builder.build();
  }

  private static Block varcharBlockWithNull(String... values) {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, values.length);
    for (String v : values) {
      if (v == null) {
        builder.appendNull();
      } else {
        VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(v));
      }
    }
    return builder.build();
  }

  private static Block bigintBlock(long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (long v : values) {
      BigintType.BIGINT.writeLong(builder, v);
    }
    return builder.build();
  }

  private static void assertVarchar(String expected, Block block, int position) {
    assertFalse(block.isNull(position), "Expected non-null at position " + position);
    assertEquals(expected, VarcharType.VARCHAR.getSlice(block, position).toStringUtf8());
  }
}
