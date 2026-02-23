/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.mapping;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DateFormatResolver")
class DateFormatResolverTest {

  private final DateFormatResolver resolver = new DateFormatResolver();

  @Nested
  @DisplayName("Precision resolution")
  class PrecisionResolution {

    @Test
    @DisplayName("Null format returns millis precision with ISO formatter")
    void nullFormat() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve(null);
      assertNotNull(info.getFormatter());
      assertEquals(3, info.getTimestampPrecision());
    }

    @Test
    @DisplayName("Empty format returns millis precision")
    void emptyFormat() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("");
      assertEquals(3, info.getTimestampPrecision());
    }

    @Test
    @DisplayName("epoch_millis -> precision 3")
    void epochMillis() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("epoch_millis");
      assertEquals(3, info.getTimestampPrecision());
      assertNotNull(info.getFormatter());
    }

    @Test
    @DisplayName("epoch_second -> precision 3")
    void epochSecond() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("epoch_second");
      assertEquals(3, info.getTimestampPrecision());
    }

    @Test
    @DisplayName("strict_date_optional_time -> precision 3")
    void strictDateOptionalTime() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("strict_date_optional_time");
      assertEquals(3, info.getTimestampPrecision());
    }

    @Test
    @DisplayName("strict_date_optional_time_nanos -> precision 9")
    void strictDateOptionalTimeNanos() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("strict_date_optional_time_nanos");
      assertEquals(9, info.getTimestampPrecision());
    }

    @Test
    @DisplayName("date format returns precision 3")
    void dateFormat() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("date");
      assertEquals(3, info.getTimestampPrecision());
    }

    @Test
    @DisplayName("strict_date_time -> precision 3")
    void strictDateTime() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("strict_date_time");
      assertEquals(3, info.getTimestampPrecision());
    }

    @Test
    @DisplayName("Custom Java date pattern")
    void customPattern() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("yyyy-MM-dd HH:mm:ss");
      assertNotNull(info.getFormatter());
      assertEquals(3, info.getTimestampPrecision());
    }

    @Test
    @DisplayName("Invalid custom pattern falls back gracefully")
    void invalidPattern() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("not_a_real_format_QQQQQ");
      assertNotNull(info.getFormatter());
      assertEquals(3, info.getTimestampPrecision());
    }
  }

  @Nested
  @DisplayName("Composite formats")
  class CompositeFormats {

    @Test
    @DisplayName("Composite format with || separator")
    void compositeFormat() {
      DateFormatResolver.DateFormatInfo info =
          resolver.resolve("strict_date_optional_time||epoch_millis");
      assertNotNull(info.getFormatter());
      assertEquals(3, info.getTimestampPrecision());
    }

    @Test
    @DisplayName("Composite format with nanos variant picks max precision")
    void compositeWithNanos() {
      DateFormatResolver.DateFormatInfo info =
          resolver.resolve("strict_date_optional_time_nanos||epoch_millis");
      assertEquals(9, info.getTimestampPrecision());
    }
  }

  @Nested
  @DisplayName("Epoch formatters are structurally distinct")
  class EpochFormatters {

    @Test
    @DisplayName("epoch_millis and epoch_second return different formatters")
    void epochMillisVsEpochSecondDifferentFormatters() {
      DateFormatResolver.DateFormatInfo millisInfo = resolver.resolve("epoch_millis");
      DateFormatResolver.DateFormatInfo secondInfo = resolver.resolve("epoch_second");
      // They should be structurally different formatter instances
      assertNotSame(millisInfo.getFormatter(), secondInfo.getFormatter());
    }

    @Test
    @DisplayName("epoch_second formatter can parse seconds-since-epoch with fractional part")
    void epochSecondParsesFractional() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("epoch_second");
      // 1620000000.123 = 2021-05-03T00:00:00.123Z
      Instant parsed = DateFormatResolver.parseDate("1620000000.123", info);
      assertEquals(1620000000L, parsed.getEpochSecond());
      assertEquals(123000000, parsed.getNano());
    }

    @Test
    @DisplayName("epoch_second formatter can parse whole seconds")
    void epochSecondParsesWhole() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("epoch_second");
      Instant parsed = DateFormatResolver.parseDate("1620000000", info);
      assertEquals(1620000000L, parsed.getEpochSecond());
      assertEquals(0, parsed.getNano());
    }
  }

  @Nested
  @DisplayName("ISO format parsing")
  class IsoFormatParsing {

    @Test
    @DisplayName("strict_date_optional_time formatter parses ISO datetime")
    void strictDateOptionalTimeParses() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("strict_date_optional_time");
      // Parse a standard ISO datetime string
      ZonedDateTime zdt = ZonedDateTime.parse("2021-05-03T12:30:45.123Z", info.getFormatter());
      assertEquals(2021, zdt.getYear());
      assertEquals(5, zdt.getMonthValue());
      assertEquals(3, zdt.getDayOfMonth());
      assertEquals(12, zdt.getHour());
      assertEquals(30, zdt.getMinute());
      assertEquals(45, zdt.getSecond());
    }

    @Test
    @DisplayName("Custom pattern formatter parses matching date string")
    void customPatternParses() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("yyyy-MM-dd HH:mm:ss");
      var parsed = info.getFormatter().parse("2023-06-15 14:30:00");
      assertNotNull(parsed);
    }

    @Test
    @DisplayName("date formatter parses ISO local date")
    void dateFormatterParsesLocalDate() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve("date");
      var parsed = info.getFormatter().parse("2023-01-15");
      assertNotNull(parsed);
    }

    @Test
    @DisplayName("Default (null) formatter parses ISO date-time")
    void defaultFormatterParses() {
      DateFormatResolver.DateFormatInfo info = resolver.resolve(null);
      ZonedDateTime zdt =
          ZonedDateTime.parse("2021-05-03T12:30:45Z", info.getFormatter().withZone(ZoneOffset.UTC));
      assertEquals(2021, zdt.getYear());
    }
  }
}
