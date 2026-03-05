/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.opensearch.sql.dqe.function.scalar.DateTimeFunctions;
import org.opensearch.sql.dqe.function.scalar.MathFunctions;
import org.opensearch.sql.dqe.function.scalar.ScalarFunctionImplementation;
import org.opensearch.sql.dqe.function.scalar.StringFunctions;
import org.opensearch.sql.dqe.function.scalar.TrigFunctions;

/** Creates and populates a {@link FunctionRegistry} with all built-in scalar functions. */
public final class BuiltinFunctions {

  private BuiltinFunctions() {}

  /** Create a new registry containing all built-in functions. */
  public static FunctionRegistry createRegistry() {
    FunctionRegistry registry = new FunctionRegistry();
    registerStringFunctions(registry);
    registerMathFunctions(registry);
    registerDateTimeFunctions(registry);
    registerTrigFunctions(registry);
    return registry;
  }

  private static void registerStringFunctions(FunctionRegistry r) {
    reg(r, "upper", List.of(VarcharType.VARCHAR), VarcharType.VARCHAR, StringFunctions.upper());
    reg(r, "lower", List.of(VarcharType.VARCHAR), VarcharType.VARCHAR, StringFunctions.lower());
    reg(r, "length", List.of(VarcharType.VARCHAR), BigintType.BIGINT, StringFunctions.length());
    reg(
        r,
        "concat",
        List.of(VarcharType.VARCHAR, VarcharType.VARCHAR),
        VarcharType.VARCHAR,
        StringFunctions.concat());
    reg(
        r,
        "substring",
        List.of(VarcharType.VARCHAR, BigintType.BIGINT, BigintType.BIGINT),
        VarcharType.VARCHAR,
        StringFunctions.substring());
    reg(
        r,
        "substr",
        List.of(VarcharType.VARCHAR, BigintType.BIGINT, BigintType.BIGINT),
        VarcharType.VARCHAR,
        StringFunctions.substring());
    reg(r, "trim", List.of(VarcharType.VARCHAR), VarcharType.VARCHAR, StringFunctions.trim());
    reg(r, "ltrim", List.of(VarcharType.VARCHAR), VarcharType.VARCHAR, StringFunctions.ltrim());
    reg(r, "rtrim", List.of(VarcharType.VARCHAR), VarcharType.VARCHAR, StringFunctions.rtrim());
    reg(
        r,
        "replace",
        List.of(VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR),
        VarcharType.VARCHAR,
        StringFunctions.replace());
    reg(
        r,
        "position",
        List.of(VarcharType.VARCHAR, VarcharType.VARCHAR),
        BigintType.BIGINT,
        StringFunctions.position());
    reg(r, "reverse", List.of(VarcharType.VARCHAR), VarcharType.VARCHAR, StringFunctions.reverse());
    reg(
        r,
        "lpad",
        List.of(VarcharType.VARCHAR, BigintType.BIGINT, VarcharType.VARCHAR),
        VarcharType.VARCHAR,
        StringFunctions.lpad());
    reg(
        r,
        "rpad",
        List.of(VarcharType.VARCHAR, BigintType.BIGINT, VarcharType.VARCHAR),
        VarcharType.VARCHAR,
        StringFunctions.rpad());
    reg(
        r,
        "starts_with",
        List.of(VarcharType.VARCHAR, VarcharType.VARCHAR),
        BooleanType.BOOLEAN,
        StringFunctions.startsWith());
    reg(r, "chr", List.of(BigintType.BIGINT), VarcharType.VARCHAR, StringFunctions.chr());
    reg(
        r,
        "codepoint",
        List.of(VarcharType.VARCHAR),
        BigintType.BIGINT,
        StringFunctions.codepoint());
  }

  private static void registerMathFunctions(FunctionRegistry r) {
    reg(r, "abs", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.abs());
    reg(r, "ceil", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.ceil());
    reg(r, "ceiling", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.ceil());
    reg(r, "floor", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.floor());
    reg(
        r,
        "round",
        List.of(DoubleType.DOUBLE, BigintType.BIGINT),
        DoubleType.DOUBLE,
        MathFunctions.round());
    reg(
        r,
        "truncate",
        List.of(DoubleType.DOUBLE, BigintType.BIGINT),
        DoubleType.DOUBLE,
        MathFunctions.truncate());
    reg(
        r,
        "power",
        List.of(DoubleType.DOUBLE, DoubleType.DOUBLE),
        DoubleType.DOUBLE,
        MathFunctions.power());
    reg(
        r,
        "pow",
        List.of(DoubleType.DOUBLE, DoubleType.DOUBLE),
        DoubleType.DOUBLE,
        MathFunctions.power());
    reg(r, "sqrt", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.sqrt());
    reg(r, "cbrt", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.cbrt());
    reg(r, "exp", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.exp());
    reg(r, "ln", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.ln());
    reg(r, "log2", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.log2());
    reg(r, "log10", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.log10());
    reg(
        r,
        "mod",
        List.of(DoubleType.DOUBLE, DoubleType.DOUBLE),
        DoubleType.DOUBLE,
        MathFunctions.mod());
    reg(r, "sign", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.sign());
    reg(r, "pi", List.of(), DoubleType.DOUBLE, MathFunctions.pi());
    reg(r, "e", List.of(), DoubleType.DOUBLE, MathFunctions.e());
    reg(r, "radians", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.radians());
    reg(r, "degrees", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, MathFunctions.degrees());
  }

  private static void registerDateTimeFunctions(FunctionRegistry r) {
    reg(
        r,
        "year",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        BigintType.BIGINT,
        DateTimeFunctions.year());
    reg(
        r,
        "month",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        BigintType.BIGINT,
        DateTimeFunctions.month());
    reg(
        r,
        "day",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        BigintType.BIGINT,
        DateTimeFunctions.day());
    reg(
        r,
        "hour",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        BigintType.BIGINT,
        DateTimeFunctions.hour());
    reg(
        r,
        "minute",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        BigintType.BIGINT,
        DateTimeFunctions.minute());
    reg(
        r,
        "second",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        BigintType.BIGINT,
        DateTimeFunctions.second());
    reg(
        r,
        "day_of_week",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        BigintType.BIGINT,
        DateTimeFunctions.dayOfWeek());
    reg(
        r,
        "dow",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        BigintType.BIGINT,
        DateTimeFunctions.dayOfWeek());
    reg(
        r,
        "day_of_year",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        BigintType.BIGINT,
        DateTimeFunctions.dayOfYear());
    reg(
        r,
        "doy",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        BigintType.BIGINT,
        DateTimeFunctions.dayOfYear());
    reg(r, "now", List.of(), TimestampType.TIMESTAMP_MILLIS, DateTimeFunctions.now());
    reg(r, "current_timestamp", List.of(), TimestampType.TIMESTAMP_MILLIS, DateTimeFunctions.now());
    reg(
        r,
        "from_unixtime",
        List.of(DoubleType.DOUBLE),
        TimestampType.TIMESTAMP_MILLIS,
        DateTimeFunctions.fromUnixtime());
    reg(
        r,
        "to_unixtime",
        List.of(TimestampType.TIMESTAMP_MILLIS),
        DoubleType.DOUBLE,
        DateTimeFunctions.toUnixtime());
  }

  private static void registerTrigFunctions(FunctionRegistry r) {
    reg(r, "sin", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, TrigFunctions.sin());
    reg(r, "cos", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, TrigFunctions.cos());
    reg(r, "tan", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, TrigFunctions.tan());
    reg(r, "asin", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, TrigFunctions.asin());
    reg(r, "acos", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, TrigFunctions.acos());
    reg(r, "atan", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, TrigFunctions.atan());
    reg(
        r,
        "atan2",
        List.of(DoubleType.DOUBLE, DoubleType.DOUBLE),
        DoubleType.DOUBLE,
        TrigFunctions.atan2());
  }

  private static void reg(
      FunctionRegistry registry,
      String name,
      List<Type> argTypes,
      Type returnType,
      ScalarFunctionImplementation impl) {
    registry.register(
        FunctionMetadata.builder()
            .name(name)
            .argumentTypes(argTypes)
            .returnType(returnType)
            .kind(FunctionKind.SCALAR)
            .scalarImplementation(impl)
            .build());
  }
}
