/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.scalar;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Date/time scalar functions operating on Trino TIMESTAMP(3) blocks. Timestamps are stored as epoch
 * microseconds internally.
 */
public final class DateTimeFunctions {

  private DateTimeFunctions() {}

  /** Extracts the year from a timestamp. */
  public static ScalarFunctionImplementation year() {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          long micros = TimestampType.TIMESTAMP_MILLIS.getLong(input, pos);
          ZonedDateTime dt = Instant.ofEpochMilli(micros / 1000).atZone(ZoneOffset.UTC);
          BigintType.BIGINT.writeLong(builder, dt.getYear());
        }
      }
      return builder.build();
    };
  }

  /** Extracts the month (1-12) from a timestamp. */
  public static ScalarFunctionImplementation month() {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          long micros = TimestampType.TIMESTAMP_MILLIS.getLong(input, pos);
          ZonedDateTime dt = Instant.ofEpochMilli(micros / 1000).atZone(ZoneOffset.UTC);
          BigintType.BIGINT.writeLong(builder, dt.getMonthValue());
        }
      }
      return builder.build();
    };
  }

  /** Extracts the day of month from a timestamp. */
  public static ScalarFunctionImplementation day() {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          long micros = TimestampType.TIMESTAMP_MILLIS.getLong(input, pos);
          ZonedDateTime dt = Instant.ofEpochMilli(micros / 1000).atZone(ZoneOffset.UTC);
          BigintType.BIGINT.writeLong(builder, dt.getDayOfMonth());
        }
      }
      return builder.build();
    };
  }

  /** Extracts the hour (0-23) from a timestamp. */
  public static ScalarFunctionImplementation hour() {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          long micros = TimestampType.TIMESTAMP_MILLIS.getLong(input, pos);
          ZonedDateTime dt = Instant.ofEpochMilli(micros / 1000).atZone(ZoneOffset.UTC);
          BigintType.BIGINT.writeLong(builder, dt.getHour());
        }
      }
      return builder.build();
    };
  }

  /** Extracts the minute from a timestamp. */
  public static ScalarFunctionImplementation minute() {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          long micros = TimestampType.TIMESTAMP_MILLIS.getLong(input, pos);
          ZonedDateTime dt = Instant.ofEpochMilli(micros / 1000).atZone(ZoneOffset.UTC);
          BigintType.BIGINT.writeLong(builder, dt.getMinute());
        }
      }
      return builder.build();
    };
  }

  /** Extracts the second from a timestamp. */
  public static ScalarFunctionImplementation second() {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          long micros = TimestampType.TIMESTAMP_MILLIS.getLong(input, pos);
          ZonedDateTime dt = Instant.ofEpochMilli(micros / 1000).atZone(ZoneOffset.UTC);
          BigintType.BIGINT.writeLong(builder, dt.getSecond());
        }
      }
      return builder.build();
    };
  }

  /** Returns the day of week (1=Monday to 7=Sunday) from a timestamp. */
  public static ScalarFunctionImplementation dayOfWeek() {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          long micros = TimestampType.TIMESTAMP_MILLIS.getLong(input, pos);
          ZonedDateTime dt = Instant.ofEpochMilli(micros / 1000).atZone(ZoneOffset.UTC);
          BigintType.BIGINT.writeLong(builder, dt.getDayOfWeek().getValue());
        }
      }
      return builder.build();
    };
  }

  /** Returns the day of year from a timestamp. */
  public static ScalarFunctionImplementation dayOfYear() {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          long micros = TimestampType.TIMESTAMP_MILLIS.getLong(input, pos);
          ZonedDateTime dt = Instant.ofEpochMilli(micros / 1000).atZone(ZoneOffset.UTC);
          BigintType.BIGINT.writeLong(builder, dt.getDayOfYear());
        }
      }
      return builder.build();
    };
  }

  /** Returns the current timestamp for all positions. Zero-arg function. */
  public static ScalarFunctionImplementation now() {
    return (args, positionCount) -> {
      long currentMicros = System.currentTimeMillis() * 1000;
      BlockBuilder builder = TimestampType.TIMESTAMP_MILLIS.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, currentMicros);
      }
      return builder.build();
    };
  }

  /**
   * Converts a DOUBLE epoch-seconds value to a timestamp. Multiplies by 1,000,000 to produce epoch
   * microseconds.
   */
  public static ScalarFunctionImplementation fromUnixtime() {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = TimestampType.TIMESTAMP_MILLIS.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          double epochSeconds = DoubleType.DOUBLE.getDouble(input, pos);
          long micros = (long) (epochSeconds * 1_000_000);
          TimestampType.TIMESTAMP_MILLIS.writeLong(builder, micros);
        }
      }
      return builder.build();
    };
  }

  /** Converts a timestamp to a DOUBLE epoch-seconds value. Divides micros by 1,000,000.0. */
  public static ScalarFunctionImplementation toUnixtime() {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          long micros = TimestampType.TIMESTAMP_MILLIS.getLong(input, pos);
          DoubleType.DOUBLE.writeDouble(builder, micros / 1_000_000.0);
        }
      }
      return builder.build();
    };
  }

  /**
   * DATE_TRUNC(unit, timestamp) — truncates a timestamp to the specified unit. The unit argument is
   * a VARCHAR ('second', 'minute', 'hour', 'day', 'month', 'year'). Returns a timestamp.
   */
  public static ScalarFunctionImplementation dateTrunc() {
    return (args, positionCount) -> {
      Block unitBlock = args[0];
      Block tsBlock = args[1];
      BlockBuilder builder = TimestampType.TIMESTAMP_MILLIS.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (tsBlock.isNull(pos) || unitBlock.isNull(pos)) {
          builder.appendNull();
        } else {
          String unit = VarcharType.VARCHAR.getSlice(unitBlock, pos).toStringUtf8().toLowerCase();
          long micros = TimestampType.TIMESTAMP_MILLIS.getLong(tsBlock, pos);
          long truncatedMicros = truncateTimestamp(micros, unit);
          TimestampType.TIMESTAMP_MILLIS.writeLong(builder, truncatedMicros);
        }
      }
      return builder.build();
    };
  }

  private static long truncateTimestamp(long micros, String unit) {
    java.time.Instant instant = java.time.Instant.ofEpochSecond(0, micros * 1000L);
    java.time.ZonedDateTime zdt = instant.atZone(java.time.ZoneOffset.UTC);
    switch (unit) {
      case "second":
        zdt = zdt.withNano(0);
        break;
      case "minute":
        zdt = zdt.withSecond(0).withNano(0);
        break;
      case "hour":
        zdt = zdt.withMinute(0).withSecond(0).withNano(0);
        break;
      case "day":
        zdt = zdt.toLocalDate().atStartOfDay(java.time.ZoneOffset.UTC);
        break;
      case "month":
        zdt = zdt.withDayOfMonth(1).toLocalDate().atStartOfDay(java.time.ZoneOffset.UTC);
        break;
      case "year":
        zdt =
            zdt.withMonth(1).withDayOfMonth(1).toLocalDate().atStartOfDay(java.time.ZoneOffset.UTC);
        break;
      default:
        // Return unchanged for unsupported units
        return micros;
    }
    return zdt.toInstant().getEpochSecond() * 1_000_000L + zdt.toInstant().getNano() / 1_000L;
  }
}
