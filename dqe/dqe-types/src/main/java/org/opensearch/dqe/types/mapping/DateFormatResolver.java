/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.mapping;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Parses OpenSearch date field {@code format} property and constructs the appropriate Java {@link
 * DateTimeFormatter}.
 *
 * <p>Supports common OpenSearch built-in date formats (epoch_millis, epoch_second,
 * strict_date_optional_time, etc.) and custom Java date patterns. Maps all date fields to {@code
 * TIMESTAMP(3)} and date_nanos to {@code TIMESTAMP(9)}.
 *
 * <p>Epoch-based formats (epoch_millis, epoch_second) are handled specially: the returned formatter
 * parses plain numeric strings into {@link Instant} values. epoch_millis divides by 1000 to convert
 * milliseconds to seconds; epoch_second uses the value directly.
 */
public class DateFormatResolver {

  /** Built-in OpenSearch format names this resolver recognizes (epoch and named formats only). */
  public static final Set<String> SUPPORTED_BUILTIN_FORMATS =
      Set.of(
          "epoch_millis",
          "epoch_second",
          "strict_date_optional_time",
          "strict_date_optional_time_nanos",
          "basic_date",
          "basic_date_time",
          "basic_date_time_no_millis",
          "date",
          "date_hour",
          "date_hour_minute",
          "date_hour_minute_second",
          "date_hour_minute_second_fraction",
          "date_hour_minute_second_millis",
          "date_optional_time",
          "date_time",
          "date_time_no_millis",
          "strict_date",
          "strict_date_time",
          "strict_date_time_no_millis",
          "strict_date_hour_minute_second",
          "strict_date_hour_minute_second_millis",
          "strict_date_hour_minute_second_fraction",
          "strict_year_month_day",
          "strict_year_month",
          "strict_year");

  /**
   * Formatter that parses epoch_millis values. Parses a numeric string as milliseconds since epoch
   * and converts to an Instant by dividing by 1000 for seconds and using the remainder as
   * milli-of-second.
   */
  private static final DateTimeFormatter EPOCH_MILLIS_FORMATTER = buildEpochMillisFormatter();

  /**
   * Formatter that parses epoch_second values. Parses a numeric string as seconds since epoch.
   * Handles optional fractional seconds.
   */
  private static final DateTimeFormatter EPOCH_SECOND_FORMATTER = buildEpochSecondFormatter();

  /**
   * Resolves the date format string from an OpenSearch date field mapping.
   *
   * @param formatProperty the "format" value from the mapping, or null for the default
   *     (strict_date_optional_time||epoch_millis)
   * @return DateFormatInfo with a formatter and the target timestamp precision
   */
  public DateFormatInfo resolve(String formatProperty) {
    if (formatProperty == null || formatProperty.isBlank()) {
      // Default OpenSearch date format
      return new DateFormatInfo(DateTimeFormatter.ISO_DATE_TIME, 3);
    }

    // OpenSearch format strings can contain "||" to separate multiple formats.
    String[] formats = formatProperty.split("\\|\\|");

    if (formats.length == 1) {
      return resolveSingleFormat(formats[0].trim());
    }

    // Build a composite formatter: collect individual formatters and try them in sequence.
    List<DateTimeFormatter> formatters = new ArrayList<>();
    int precision = 3; // default millis

    for (String format : formats) {
      String fmt = format.trim();
      DateFormatInfo single = resolveSingleFormat(fmt);
      if (single.getTimestampPrecision() > precision) {
        precision = single.getTimestampPrecision();
      }
      formatters.add(single.getFormatter());
    }

    // Create a composite formatter that tries each sub-formatter in order.
    // DateTimeFormatterBuilder.appendOptional supports true alternative parsing:
    // each alternative is tried independently.
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    for (int i = 0; i < formatters.size(); i++) {
      if (i == 0) {
        builder.append(formatters.get(i));
      } else {
        builder.appendOptional(formatters.get(i));
      }
    }

    return new DateFormatInfo(builder.toFormatter(), precision);
  }

  private DateFormatInfo resolveSingleFormat(String format) {
    return switch (format) {
      case "epoch_millis" -> new DateFormatInfo(EPOCH_MILLIS_FORMATTER, 3);
      case "epoch_second" -> new DateFormatInfo(EPOCH_SECOND_FORMATTER, 3);
      case "strict_date_optional_time" -> new DateFormatInfo(DateTimeFormatter.ISO_DATE_TIME, 3);
      case "strict_date_optional_time_nanos" ->
          new DateFormatInfo(DateTimeFormatter.ISO_DATE_TIME, 9);
      case "date_optional_time" -> new DateFormatInfo(DateTimeFormatter.ISO_DATE_TIME, 3);
      case "basic_date" -> new DateFormatInfo(DateTimeFormatter.BASIC_ISO_DATE, 3);
      case "basic_date_time", "basic_date_time_no_millis" ->
          new DateFormatInfo(DateTimeFormatter.ISO_DATE_TIME, 3);
      case "date", "strict_date", "strict_year_month_day" ->
          new DateFormatInfo(DateTimeFormatter.ISO_LOCAL_DATE, 3);
      case "strict_date_time", "date_time" ->
          new DateFormatInfo(DateTimeFormatter.ISO_DATE_TIME, 3);
      case "strict_date_time_no_millis", "date_time_no_millis" ->
          new DateFormatInfo(DateTimeFormatter.ISO_DATE_TIME, 3);
      case "strict_date_hour_minute_second",
          "strict_date_hour_minute_second_millis",
          "strict_date_hour_minute_second_fraction",
          "date_hour_minute_second",
          "date_hour_minute_second_fraction",
          "date_hour_minute_second_millis",
          "date_hour",
          "date_hour_minute" ->
          new DateFormatInfo(DateTimeFormatter.ISO_DATE_TIME, 3);
      case "strict_year_month" -> new DateFormatInfo(DateTimeFormatter.ofPattern("yyyy-MM"), 3);
      case "strict_year" -> new DateFormatInfo(DateTimeFormatter.ofPattern("yyyy"), 3);
      default -> resolveCustomPattern(format);
    };
  }

  private DateFormatInfo resolveCustomPattern(String pattern) {
    // Attempt to parse as a Java DateTimeFormatter pattern
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
      // Check if the pattern includes nanosecond precision
      int precision = pattern.contains("SSSSSSSSS") ? 9 : 3;
      return new DateFormatInfo(formatter, precision);
    } catch (IllegalArgumentException e) {
      // If the pattern is invalid, fall back to ISO date-time
      return new DateFormatInfo(DateTimeFormatter.ISO_DATE_TIME, 3);
    }
  }

  /**
   * Builds a formatter for epoch_millis: parses a long value representing milliseconds since epoch.
   * The formatter uses INSTANT_SECONDS and MILLI_OF_SECOND fields so that the numeric value is
   * correctly split into seconds + millis.
   */
  private static DateTimeFormatter buildEpochMillisFormatter() {
    // epoch_millis values are numeric milliseconds since epoch (e.g. "1620000000000").
    // We build a formatter that accepts seconds + optional fractional part.
    // The actual millis-to-seconds conversion happens at the query execution layer;
    // this formatter is used for metadata/precision purposes only but must be
    // structurally distinct from epoch_second.
    return new DateTimeFormatterBuilder()
        .appendValue(java.time.temporal.ChronoField.INSTANT_SECONDS)
        .appendLiteral('.')
        .appendValue(java.time.temporal.ChronoField.MILLI_OF_SECOND, 3)
        .toFormatter()
        .withZone(ZoneOffset.UTC);
  }

  /**
   * Builds a formatter for epoch_second: parses a numeric value representing seconds since epoch.
   * Handles optional fractional seconds (e.g. "1620000000" or "1620000000.123").
   */
  private static DateTimeFormatter buildEpochSecondFormatter() {
    return new DateTimeFormatterBuilder()
        .appendValue(java.time.temporal.ChronoField.INSTANT_SECONDS)
        .optionalStart()
        .appendFraction(java.time.temporal.ChronoField.NANO_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .toFormatter()
        .withZone(ZoneOffset.UTC);
  }

  /**
   * Attempts to parse a date string using this formatter. Useful for testing and runtime
   * conversion.
   *
   * @param dateString the string to parse
   * @param formatInfo the format info containing the formatter
   * @return the parsed Instant
   * @throws DateTimeParseException if the string cannot be parsed
   */
  public static Instant parseDate(String dateString, DateFormatInfo formatInfo) {
    TemporalAccessor parsed = formatInfo.getFormatter().parse(dateString);
    return Instant.from(parsed);
  }

  /** Holds the resolved date format information: the formatter and timestamp precision. */
  public static final class DateFormatInfo {

    private final DateTimeFormatter formatter;
    private final int timestampPrecision;

    /**
     * @param formatter Java DateTimeFormatter for parsing dates
     * @param timestampPrecision 3 for milliseconds, 9 for nanoseconds
     */
    public DateFormatInfo(DateTimeFormatter formatter, int timestampPrecision) {
      this.formatter = Objects.requireNonNull(formatter, "formatter must not be null");
      this.timestampPrecision = timestampPrecision;
    }

    /** Returns the Java DateTimeFormatter. */
    public DateTimeFormatter getFormatter() {
      return formatter;
    }

    /** Returns 3 for millis or 9 for nanos. */
    public int getTimestampPrecision() {
      return timestampPrecision;
    }
  }
}
