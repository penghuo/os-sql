/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_WITHOUT_NANO;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionProperties;

/** Expression Timestamp Value in UTC timezone */
@RequiredArgsConstructor
public class ExprTimestampValue extends AbstractExprValue {

  private final Instant timestamp;

  /**
   * Construct ExprTimestamp Value with default UTC timezone.
   *
   * @param timestamp
   */
  public ExprTimestampValue(String timestamp) {
    this(timestamp, ZoneOffset.UTC);
  }

  /**
   * Construct ExprTimestamp Value with timeZone
   *
   * @param timestamp timestamp
   * @param timeZone time zone
   */
  public ExprTimestampValue(String timestamp, ZoneId timeZone) {
    try {
      this.timestamp =
          LocalDateTime.parse(timestamp, DATE_TIME_FORMATTER_VARIABLE_NANOS)
              .atZone(timeZone)
              .toInstant();
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(
          String.format(
              "timestamp:%s in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'",
              timestamp));
    }
  }

  /** localDateTime Constructor. */
  public ExprTimestampValue(LocalDateTime localDateTime) {
    this.timestamp = localDateTime.atZone(ZoneOffset.UTC).toInstant();
  }

  /** localDateTime Constructor with timezone awareness. */
  public ExprTimestampValue(LocalDateTime localDateTime, ZoneId timezone) {
    this.timestamp = localDateTime.atZone(timezone).toInstant();
  }

  @Override
  public String value() {
    return value(ZoneOffset.UTC);
  }

  /**
   * Get string representation in specified timezone. This is where timezone formatting should be
   * applied per user's architecture suggestion.
   */
  public String value(ZoneId timeZone) {
    return timestamp.getNano() == 0
        ? DATE_TIME_FORMATTER_WITHOUT_NANO
            .withZone(timeZone)
            .format(timestamp.truncatedTo(ChronoUnit.SECONDS))
        : DATE_TIME_FORMATTER_VARIABLE_NANOS.withZone(timeZone).format(timestamp);
  }

  @Override
  public ExprType type() {
    return ExprCoreType.TIMESTAMP;
  }

  public Instant timestampValue() {
    return timestamp;
  }

  /**
   * Get LocalData at specified timeZone.
   */
  @Override
  public LocalDate dateValue(FunctionProperties funcProp) {
    return timestamp.atZone(funcProp.getSessionTimeZone()).toLocalDate();
  }

  @Override
  public LocalTime timeValue(FunctionProperties funcProp) {
    return timestamp.atZone(funcProp.getSessionTimeZone()).toLocalTime();
  }

  @Override
  public boolean isDateTime() {
    return true;
  }

  @Override
  public String toString() {
    return String.format("TIMESTAMP '%s'", value());
  }

  @Override
  public int compare(ExprValue other) {
    if (other instanceof ExprTimestampValue) {
      return timestamp.compareTo(((ExprTimestampValue) other).timestampValue());
    } else {
      return 1;
    }
  }

  @Override
  public boolean equal(ExprValue other) {
    if (other instanceof ExprTimestampValue) {
      return timestamp.equals(((ExprTimestampValue) other).timestampValue());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(timestamp);
  }
}
