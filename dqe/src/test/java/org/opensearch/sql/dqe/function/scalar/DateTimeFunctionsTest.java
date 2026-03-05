/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.scalar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for {@link DateTimeFunctions}. */
@DisplayName("DateTimeFunctions")
class DateTimeFunctionsTest {

  // March 15, 2024 10:30:45 UTC stored as epoch microseconds
  private static final long EPOCH_MICROS =
      LocalDateTime.of(2024, 3, 15, 10, 30, 45).toInstant(ZoneOffset.UTC).toEpochMilli() * 1000;

  @Test
  @DisplayName("year extracts 2024")
  void yearExtraction() {
    Block result = DateTimeFunctions.year().evaluate(new Block[] {buildTimestampBlock()}, 1);
    assertEquals(2024L, BigintType.BIGINT.getLong(result, 0));
  }

  @Test
  @DisplayName("month extracts 3")
  void monthExtraction() {
    Block result = DateTimeFunctions.month().evaluate(new Block[] {buildTimestampBlock()}, 1);
    assertEquals(3L, BigintType.BIGINT.getLong(result, 0));
  }

  @Test
  @DisplayName("day extracts 15")
  void dayExtraction() {
    Block result = DateTimeFunctions.day().evaluate(new Block[] {buildTimestampBlock()}, 1);
    assertEquals(15L, BigintType.BIGINT.getLong(result, 0));
  }

  @Test
  @DisplayName("hour extracts 10")
  void hourExtraction() {
    Block result = DateTimeFunctions.hour().evaluate(new Block[] {buildTimestampBlock()}, 1);
    assertEquals(10L, BigintType.BIGINT.getLong(result, 0));
  }

  @Test
  @DisplayName("minute extracts 30")
  void minuteExtraction() {
    Block result = DateTimeFunctions.minute().evaluate(new Block[] {buildTimestampBlock()}, 1);
    assertEquals(30L, BigintType.BIGINT.getLong(result, 0));
  }

  @Test
  @DisplayName("second extracts 45")
  void secondExtraction() {
    Block result = DateTimeFunctions.second().evaluate(new Block[] {buildTimestampBlock()}, 1);
    assertEquals(45L, BigintType.BIGINT.getLong(result, 0));
  }

  @Test
  @DisplayName("dayOfWeek returns 5 for Friday (2024-03-15)")
  void dayOfWeekExtraction() {
    Block result = DateTimeFunctions.dayOfWeek().evaluate(new Block[] {buildTimestampBlock()}, 1);
    // March 15, 2024 is a Friday = 5
    assertEquals(5L, BigintType.BIGINT.getLong(result, 0));
  }

  @Test
  @DisplayName("dayOfYear returns 75 for March 15 in leap year 2024")
  void dayOfYearExtraction() {
    Block result = DateTimeFunctions.dayOfYear().evaluate(new Block[] {buildTimestampBlock()}, 1);
    // 2024 is a leap year: Jan(31) + Feb(29) + 15 = 75
    assertEquals(75L, BigintType.BIGINT.getLong(result, 0));
  }

  @Test
  @DisplayName("now returns a non-zero timestamp value")
  void nowReturnsNonZero() {
    Block result = DateTimeFunctions.now().evaluate(new Block[] {}, 1);
    long micros = TimestampType.TIMESTAMP_MILLIS.getLong(result, 0);
    assertTrue(micros > 0, "now() should return a positive epoch-micros value");
  }

  @Test
  @DisplayName("fromUnixtime converts epoch seconds to timestamp micros")
  void fromUnixtimeConversion() {
    double epochSeconds = EPOCH_MICROS / 1_000_000.0;
    BlockBuilder doubleBuilder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    DoubleType.DOUBLE.writeDouble(doubleBuilder, epochSeconds);
    Block input = doubleBuilder.build();

    Block result = DateTimeFunctions.fromUnixtime().evaluate(new Block[] {input}, 1);
    long resultMicros = TimestampType.TIMESTAMP_MILLIS.getLong(result, 0);
    assertEquals(EPOCH_MICROS, resultMicros);
  }

  @Test
  @DisplayName("toUnixtime converts timestamp micros to epoch seconds")
  void toUnixtimeConversion() {
    Block result = DateTimeFunctions.toUnixtime().evaluate(new Block[] {buildTimestampBlock()}, 1);
    double epochSeconds = DoubleType.DOUBLE.getDouble(result, 0);
    assertEquals(EPOCH_MICROS / 1_000_000.0, epochSeconds, 0.001);
  }

  @Test
  @DisplayName("NULL input produces null output for extraction functions")
  void nullPropagation() {
    BlockBuilder builder = TimestampType.TIMESTAMP_MILLIS.createBlockBuilder(null, 1);
    builder.appendNull();
    Block nullBlock = builder.build();

    Block yearResult = DateTimeFunctions.year().evaluate(new Block[] {nullBlock}, 1);
    assertTrue(yearResult.isNull(0));

    Block monthResult = DateTimeFunctions.month().evaluate(new Block[] {nullBlock}, 1);
    assertTrue(monthResult.isNull(0));

    Block dayResult = DateTimeFunctions.day().evaluate(new Block[] {nullBlock}, 1);
    assertTrue(dayResult.isNull(0));

    Block hourResult = DateTimeFunctions.hour().evaluate(new Block[] {nullBlock}, 1);
    assertTrue(hourResult.isNull(0));

    Block minuteResult = DateTimeFunctions.minute().evaluate(new Block[] {nullBlock}, 1);
    assertTrue(minuteResult.isNull(0));

    Block secondResult = DateTimeFunctions.second().evaluate(new Block[] {nullBlock}, 1);
    assertTrue(secondResult.isNull(0));

    Block dowResult = DateTimeFunctions.dayOfWeek().evaluate(new Block[] {nullBlock}, 1);
    assertTrue(dowResult.isNull(0));

    Block doyResult = DateTimeFunctions.dayOfYear().evaluate(new Block[] {nullBlock}, 1);
    assertTrue(doyResult.isNull(0));

    Block toUnixResult = DateTimeFunctions.toUnixtime().evaluate(new Block[] {nullBlock}, 1);
    assertTrue(toUnixResult.isNull(0));
  }

  @Test
  @DisplayName("NULL input produces null output for fromUnixtime")
  void fromUnixtimeNullPropagation() {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    builder.appendNull();
    Block nullBlock = builder.build();

    Block result = DateTimeFunctions.fromUnixtime().evaluate(new Block[] {nullBlock}, 1);
    assertTrue(result.isNull(0));
  }

  @Test
  @DisplayName("Multiple positions are processed correctly")
  void multiplePositions() {
    // Two timestamps: 2024-03-15 and epoch (1970-01-01)
    long epochZeroMicros = 0L;
    BlockBuilder builder = TimestampType.TIMESTAMP_MILLIS.createBlockBuilder(null, 2);
    TimestampType.TIMESTAMP_MILLIS.writeLong(builder, EPOCH_MICROS);
    TimestampType.TIMESTAMP_MILLIS.writeLong(builder, epochZeroMicros);
    Block tsBlock = builder.build();

    Block yearResult = DateTimeFunctions.year().evaluate(new Block[] {tsBlock}, 2);
    assertEquals(2024L, BigintType.BIGINT.getLong(yearResult, 0));
    assertEquals(1970L, BigintType.BIGINT.getLong(yearResult, 1));

    Block monthResult = DateTimeFunctions.month().evaluate(new Block[] {tsBlock}, 2);
    assertEquals(3L, BigintType.BIGINT.getLong(monthResult, 0));
    assertEquals(1L, BigintType.BIGINT.getLong(monthResult, 1));
  }

  private static Block buildTimestampBlock() {
    BlockBuilder builder = TimestampType.TIMESTAMP_MILLIS.createBlockBuilder(null, 1);
    TimestampType.TIMESTAMP_MILLIS.writeLong(builder, EPOCH_MICROS);
    return builder.build();
  }
}
