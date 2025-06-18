/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL;

import com.google.common.base.Objects;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionProperties;

/** Expression Date Value in session time zone. */
@RequiredArgsConstructor
public class ExprDateValue extends AbstractExprValue {

  private final LocalDate date;

  /** Constructor of ExprDateValue. */
  public ExprDateValue(String date) {
    try {
      this.date = LocalDate.parse(date, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
    } catch (DateTimeParseException e) {
      throw new SemanticCheckException(
          String.format("date:%s in unsupported format, please use 'yyyy-MM-dd'", date));
    }
  }

  @Override
  public String value() {
    return DateTimeFormatter.ISO_LOCAL_DATE.format(date);
  }

  @Override
  public ExprType type() {
    return ExprCoreType.DATE;
  }

  public LocalDate dateValue() {
    return date;
  }

  @Override
  public LocalTime timeValue(FunctionProperties funcProp) {
    return LocalTime.of(0, 0, 0);
  }

  /**
   * Return Instant at 00:00:00 at specified timeZone.
   * @return
   */
  @Override
  public Instant timestampValue(FunctionProperties funcProp) {
    return ZonedDateTime.of(date, timeValue(funcProp), funcProp.getSessionTimeZone()).toInstant();
  }

  @Override
  public boolean isDateTime() {
    return true;
  }

  @Override
  public String toString() {
    return String.format("DATE '%s'", value());
  }

  @Override
  public int compare(ExprValue other) {
    if (other instanceof ExprDateValue) {
      return date.compareTo(((ExprDateValue) other).dateValue());
    } else {
      return 1;
    }
  }

  @Override
  public boolean equal(ExprValue other) {
    if (other instanceof ExprDateValue) {
      return date.equals(((ExprDateValue) other).dateValue());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(date);
  }
}
