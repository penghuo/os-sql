/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.ppl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.TrinoSqlDialect;

/**
 * Custom SQL dialect for Omni's PPL-to-Trino SQL translation.
 * Handles standard Calcite operators that need Trino-specific syntax:
 * - TIMESTAMP('...') → TIMESTAMP '...' (literal prefix, not function call)
 * - EXTRACT('minute', col) → EXTRACT(MINUTE FROM col) (keyword, not string)
 *
 * os-sql custom operators (QUERY_STRING, ILIKE) bypass dialect.unparseCall()
 * and are handled as string rewrites in PplTranslator.
 */
public class OmniSqlDialect extends TrinoSqlDialect
{
    public static final OmniSqlDialect DEFAULT = new OmniSqlDialect(TrinoSqlDialect.DEFAULT_CONTEXT);

    public OmniSqlDialect(Context context)
    {
        super(context);
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec)
    {
        String opName = call.getOperator().getName();

        // JSON_OBJECT('k1', v1, 'k2', v2, ...) - Calcite emits "JSON_OBJECT(KEY 'k' VALUE v NULL ON NULL)"
        // Rewrite to Trino's  map_to_json(MAP(ARRAY[...], ARRAY[...]))
        if (opName.equalsIgnoreCase("JSON_OBJECT") || opName.equalsIgnoreCase("JSONOBJECT")) {
            // Calcite's JSON_OBJECT operator has operands: nullBehavior, key1, value1, key2, value2, ...
            int start = 1;
            int pairCount = (call.operandCount() - 1) / 2;
            if (pairCount < 1) {
                writer.print("map_to_json(MAP())");
                return;
            }
            writer.print("map_to_json(MAP(ARRAY[");
            for (int i = 0; i < pairCount; i++) {
                if (i > 0) writer.print(", ");
                call.operand(start + i * 2).unparse(writer, 0, 0);
            }
            writer.print("], ARRAY[");
            for (int i = 0; i < pairCount; i++) {
                if (i > 0) writer.print(", ");
                call.operand(start + i * 2 + 1).unparse(writer, 0, 0);
            }
            writer.print("]))");
            return;
        }

        // JSON_ARRAY(v1, v2, ...) — Calcite standard → Trino json_format(CAST(ARRAY[...] AS JSON))
        if (opName.equalsIgnoreCase("JSON_ARRAY") || opName.equalsIgnoreCase("JSONARRAY")) {
            int start = 1;
            writer.print("json_format(CAST(ARRAY[");
            for (int i = start; i < call.operandCount(); i++) {
                if (i > start) writer.print(", ");
                call.operand(i).unparse(writer, 0, 0);
            }
            writer.print("] AS JSON))");
            return;
        }

        // SPAN(value, interval, unit?)  — PPL time/numeric bucketing
        //   SPAN(ts, N, 'w'|'d'|'h'|'m'|'s'|'mo'|'y')  → date_trunc(unit, ts)  (we ignore N for single-unit buckets)
        //   SPAN(num, N)                                → floor(num / N) * N
        // Best-effort — N is dropped for time ranges.
        if (opName.equalsIgnoreCase("SPAN")) {
            if (call.operandCount() == 3 && call.operand(2) instanceof SqlCharStringLiteral unitLit) {
                String unit = unitLit.getNlsString().getValue().toLowerCase();
                String trinoUnit = switch (unit) {
                    case "y", "year", "years" -> "year";
                    case "mo", "month", "months" -> "month";
                    case "w", "week", "weeks" -> "week";
                    case "d", "day", "days" -> "day";
                    case "h", "hour", "hours" -> "hour";
                    case "m", "minute", "minutes" -> "minute";
                    case "s", "second", "seconds" -> "second";
                    default -> unit;
                };
                writer.keyword("date_trunc");
                SqlWriter.Frame frame = writer.startList("(", ")");
                writer.literal("'" + trinoUnit + "'");
                writer.sep(",");
                call.operand(0).unparse(writer, 0, 0);
                writer.endList(frame);
                return;
            }
            if (call.operandCount() == 2) {
                // SPAN(num, N) → floor(num / N) * N
                SqlWriter.Frame frame = writer.startList("(", ")");
                writer.keyword("FLOOR");
                SqlWriter.Frame floorFrame = writer.startList("(", ")");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" / ");
                call.operand(1).unparse(writer, 0, 0);
                writer.endList(floorFrame);
                writer.print(" * ");
                call.operand(1).unparse(writer, 0, 0);
                writer.endList(frame);
                return;
            }
        }

        // SAFE_CAST(expr AS type) → TRY_CAST(expr AS type)  (Calcite BigQuery name → Trino)
        if (opName.equalsIgnoreCase("SAFE_CAST") && call.operandCount() == 2) {
            writer.keyword("TRY_CAST");
            SqlWriter.Frame frame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.keyword("AS");
            call.operand(1).unparse(writer, 0, 0);
            writer.endList(frame);
            return;
        }

        // TIMESTAMP(literal string) → TIMESTAMP 'literal' (literal prefix)
        // TIMESTAMP(expr)          → CAST(expr AS TIMESTAMP)
        if (opName.equals("TIMESTAMP") && call.operandCount() == 1) {
            if (call.operand(0) instanceof SqlCharStringLiteral literal) {
                writer.literal("TIMESTAMP '" + literal.getNlsString().getValue() + "'");
            } else {
                writer.keyword("CAST");
                SqlWriter.Frame frame = writer.startList("(", ")");
                call.operand(0).unparse(writer, 0, 0);
                writer.keyword("AS");
                writer.keyword("TIMESTAMP");
                writer.endList(frame);
            }
            return;
        }

        // TIME(literal)  → TIME 'literal'
        // TIME(expr)     → CAST(expr AS TIME)
        if (opName.equals("TIME") && call.operandCount() == 1) {
            if (call.operand(0) instanceof SqlCharStringLiteral timeLit) {
                writer.literal("TIME '" + timeLit.getNlsString().getValue() + "'");
            } else {
                writer.keyword("CAST");
                SqlWriter.Frame frame = writer.startList("(", ")");
                call.operand(0).unparse(writer, 0, 0);
                writer.keyword("AS");
                writer.keyword("TIME");
                writer.endList(frame);
            }
            return;
        }

        // DATE(literal)  → DATE 'literal'
        // DATE(expr)     → CAST(expr AS DATE)
        if (opName.equals("DATE") && call.operandCount() == 1) {
            if (call.operand(0) instanceof SqlCharStringLiteral dateLit) {
                writer.literal("DATE '" + dateLit.getNlsString().getValue() + "'");
            } else {
                writer.keyword("CAST");
                SqlWriter.Frame frame = writer.startList("(", ")");
                call.operand(0).unparse(writer, 0, 0);
                writer.keyword("AS");
                writer.keyword("DATE");
                writer.endList(frame);
            }
            return;
        }

        // EXTRACT('minute', col) → EXTRACT(MINUTE FROM col)
        if (opName.equals("EXTRACT") && call.operandCount() == 2
                && call.operand(0) instanceof SqlCharStringLiteral unitLiteral) {
            String unit = unitLiteral.getNlsString().getValue().toUpperCase();
            writer.keyword("EXTRACT");
            SqlWriter.Frame frame = writer.startList("(", ")");
            writer.keyword(unit);
            writer.keyword("FROM");
            call.operand(1).unparse(writer, 0, 0);
            writer.endList(frame);
            return;
        }

        // MySQL-style function name → Trino canonical name mappings
        // These handle PPL function names that don't match Trino's conventions

        // PPL temporal extraction functions accept varchar/date/timestamp; Trino's don't accept varchar.
        // Wrap arg in CAST(... AS TIMESTAMP) when we emit.
        if (call.operandCount() == 1 && (
                opName.equalsIgnoreCase("DAYOFWEEK") || opName.equalsIgnoreCase("DAY_OF_WEEK")
             || opName.equalsIgnoreCase("DAYOFMONTH") || opName.equalsIgnoreCase("DAY_OF_MONTH")
             || opName.equalsIgnoreCase("DAYOFYEAR") || opName.equalsIgnoreCase("DAY_OF_YEAR")
             || opName.equalsIgnoreCase("DAY")
             || opName.equalsIgnoreCase("MONTH")
             || opName.equalsIgnoreCase("YEAR")
             || opName.equalsIgnoreCase("HOUR")
             || opName.equalsIgnoreCase("MINUTE")
             || opName.equalsIgnoreCase("SECOND")
             || opName.equalsIgnoreCase("QUARTER")
             || opName.equalsIgnoreCase("WEEK")
             || opName.equalsIgnoreCase("WEEK_OF_YEAR")
             || opName.equalsIgnoreCase("LAST_DAY"))) {
            String trinoName = switch (opName.toUpperCase()) {
                case "DAYOFWEEK" -> "day_of_week";
                case "DAYOFMONTH" -> "day";
                case "DAYOFYEAR" -> "day_of_year";
                case "LAST_DAY" -> "last_day_of_month";
                case "WEEK" -> "week_of_year";
                default -> opName.toLowerCase();
            };
            writer.keyword(trinoName);
            SqlWriter.Frame frame = writer.startList("(", ")");
            // Cast to TIMESTAMP so varchar inputs are accepted. TIMESTAMP(date) and
            // TIMESTAMP(timestamp) are both valid in Trino (identity cast).
            writer.keyword("CAST");
            SqlWriter.Frame castFrame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.keyword("AS");
            writer.keyword("TIMESTAMP");
            writer.endList(castFrame);
            writer.endList(frame);
            return;
        }

        // ARRAY(a, b, c) → ARRAY[a, b, c] — Trino uses bracket literal, not function
        if (opName.equalsIgnoreCase("ARRAY") || opName.equalsIgnoreCase("LIST")) {
            writer.print("ARRAY[");
            for (int i = 0; i < call.operandCount(); i++) {
                if (i > 0) writer.print(", ");
                call.operand(i).unparse(writer, 0, 0);
            }
            writer.print("]");
            return;
        }

        // PPL aggregate → Trino aggregate name mappings
        // FIRST(x) → arbitrary(x) — PPL first() is non-deterministic per legacy v2 semantics
        if (opName.equalsIgnoreCase("FIRST") && call.operandCount() == 1) {
            unparseFunctionLike(writer, "arbitrary", call);
            return;
        }
        // LAST(x) → arbitrary(x) — same rationale
        if (opName.equalsIgnoreCase("LAST") && call.operandCount() == 1) {
            unparseFunctionLike(writer, "arbitrary", call);
            return;
        }
        // ARG_MIN(value, sort) → min_by(value, sort)
        if (opName.equalsIgnoreCase("ARG_MIN") && call.operandCount() == 2) {
            unparseFunctionLike(writer, "min_by", call);
            return;
        }
        // ARG_MAX(value, sort) → max_by(value, sort)
        if (opName.equalsIgnoreCase("ARG_MAX") && call.operandCount() == 2) {
            unparseFunctionLike(writer, "max_by", call);
            return;
        }
        // SCALAR_MIN / SCALAR_MAX → Trino's least / greatest
        if (opName.equalsIgnoreCase("SCALAR_MIN")) {
            unparseFunctionLike(writer, "least", call);
            return;
        }
        if (opName.equalsIgnoreCase("SCALAR_MAX")) {
            unparseFunctionLike(writer, "greatest", call);
            return;
        }
        // DATEDIFF(a, b) → date_diff('day', b, a) — PPL semantics: a − b in days
        if (opName.equalsIgnoreCase("DATEDIFF") && call.operandCount() == 2) {
            writer.keyword("date_diff");
            SqlWriter.Frame frame = writer.startList("(", ")");
            writer.literal("'day'");
            writer.sep(",");
            call.operand(1).unparse(writer, 0, 0);
            writer.sep(",");
            call.operand(0).unparse(writer, 0, 0);
            writer.endList(frame);
            return;
        }
        // TIMESTAMPADD(unit, interval, ts) → date_add(lowercase_unit_literal, interval, ts)
        if (opName.equalsIgnoreCase("TIMESTAMPADD") && call.operandCount() == 3) {
            writer.keyword("date_add");
            SqlWriter.Frame frame = writer.startList("(", ")");
            // Calcite emits the unit as a special SqlIntervalQualifier keyword in operand(0);
            // fall back to printing its toString, lowercased and quoted.
            String unit = call.operand(0).toString().toLowerCase();
            writer.literal("'" + unit + "'");
            writer.sep(",");
            call.operand(1).unparse(writer, 0, 0);
            writer.sep(",");
            call.operand(2).unparse(writer, 0, 0);
            writer.endList(frame);
            return;
        }
        // REGEXP_CONTAINS(str, pattern) → regexp_like(str, pattern)
        if (opName.equalsIgnoreCase("REGEXP_CONTAINS") && call.operandCount() == 2) {
            unparseFunctionLike(writer, "regexp_like", call);
            return;
        }
        // JSON_EXTRACT_ALL(jsonStr, path) → json_extract(CAST(jsonStr AS JSON), path)
        // Trino's json_extract requires JSON type for the first arg; OpenSearch text fields are VARCHAR.
        if (opName.equalsIgnoreCase("JSON_EXTRACT_ALL") && call.operandCount() == 2) {
            writer.keyword("json_extract");
            SqlWriter.Frame frame = writer.startList("(", ")");
            writer.keyword("CAST");
            SqlWriter.Frame castFrame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.keyword("AS");
            writer.keyword("JSON");
            writer.endList(castFrame);
            writer.sep(",");
            call.operand(1).unparse(writer, 0, 0);
            writer.endList(frame);
            return;
        }
        // VALUES(x) → array_agg(x) — PPL collects distinct values as an array
        if (opName.equalsIgnoreCase("VALUES") && call.operandCount() == 1) {
            unparseFunctionLike(writer, "array_agg", call);
            return;
        }
        // TAKE(x, n) → first n values — use array_agg then slice; collapse to array_agg
        if (opName.equalsIgnoreCase("TAKE") && call.operandCount() == 2) {
            writer.keyword("slice");
            SqlWriter.Frame frame = writer.startList("(", ")");
            writer.keyword("array_agg");
            SqlWriter.Frame aggFrame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.endList(aggFrame);
            writer.sep(",");
            writer.literal("1");
            writer.sep(",");
            call.operand(1).unparse(writer, 0, 0);
            writer.endList(frame);
            return;
        }
        // MKTIME(y, m, d, h, mi, s) → date literal → Trino has no single function, omit
        // ARRAY_COMPACT(arr) → filter(arr, x -> x IS NOT NULL)  (Trino has no built-in array_compact)
        if (opName.equalsIgnoreCase("ARRAY_COMPACT") && call.operandCount() == 1) {
            writer.print("filter(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(", x -> x IS NOT NULL)");
            return;
        }
        // ARRAY_LENGTH(arr) → cardinality(arr)
        if (opName.equalsIgnoreCase("ARRAY_LENGTH") && call.operandCount() == 1) {
            unparseFunctionLike(writer, "cardinality", call);
            return;
        }
        // MVAPPEND(a, b, ...)  → concat(a, b, ...) on arrays (Trino's concat handles arrays)
        if (opName.equalsIgnoreCase("MVAPPEND")) {
            unparseFunctionLike(writer, "concat", call);
            return;
        }
        // MVFIND(arr, pattern) → Trino: element_at(filter(arr, x -> regexp_like(x, pattern)), 1)
        //   returns first matching element (or null)
        if (opName.equalsIgnoreCase("MVFIND") && call.operandCount() == 2) {
            writer.print("element_at(filter(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(", x -> regexp_like(x, ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(")), 1)");
            return;
        }
        // MVZIP(arr1, arr2) → zip(arr1, arr2) — Trino has zip() builtin
        if (opName.equalsIgnoreCase("MVZIP")) {
            unparseFunctionLike(writer, "zip", call);
            return;
        }
        // NUMBER_TO_STRING(x) → CAST(x AS VARCHAR)
        if (opName.equalsIgnoreCase("NUMBER_TO_STRING") && call.operandCount() == 1) {
            writer.print("CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS VARCHAR)");
            return;
        }
        // STRFTIME(ts_or_epoch, fmt) → Trino: format_datetime(CAST(ts_or_epoch AS TIMESTAMP), fmt)
        if (opName.equalsIgnoreCase("STRFTIME") && call.operandCount() == 2) {
            writer.print("format_datetime(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP), ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }
        // DATETIME(x) / DATETIME(x, zone)  — treat as CAST(x AS TIMESTAMP)
        if (opName.equalsIgnoreCase("DATETIME") && call.operandCount() >= 1) {
            writer.print("CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP)");
            return;
        }
        // CONVERT_TZ(ts, from, to) → at_timezone(from_tz(CAST(ts AS TIMESTAMP), from_zone), to_zone)
        // simplest robust form: AT TIME ZONE syntax
        if (opName.equalsIgnoreCase("CONVERT_TZ") && call.operandCount() == 3) {
            writer.print("(");
            writer.print("CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP) AT TIME ZONE ");
            call.operand(2).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }
        // ARRAY_SLICE(arr, start, length?) → slice(arr, start, length)
        if (opName.equalsIgnoreCase("ARRAY_SLICE")) {
            unparseFunctionLike(writer, "slice", call);
            return;
        }
        // JSON_EXTRACT_ALL(json, path1, path2, ...) with >2 operands:
        //   Trino json_extract accepts only one path; for multi-path, chain ARRAY of single extracts.
        // Single-path (2 operands) already handled above.
        if (opName.equalsIgnoreCase("JSON_EXTRACT_ALL") && call.operandCount() > 2) {
            writer.print("ARRAY[");
            for (int i = 1; i < call.operandCount(); i++) {
                if (i > 1) writer.print(", ");
                writer.print("json_extract(CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS JSON), ");
                call.operand(i).unparse(writer, 0, 0);
                writer.print(")");
            }
            writer.print("]");
            return;
        }
        // MAKEDATE(year, dayOfYear) → date_add('day', dayOfYear - 1, DATE year||'-01-01')
        if (opName.equalsIgnoreCase("MAKEDATE") && call.operandCount() == 2) {
            writer.print("date_add('day', ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(" - 1, CAST(CONCAT(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS VARCHAR), '-01-01') AS DATE))");
            return;
        }
        // MKTIME(hours) → time literal; Trino has no mktime — defer (fall through, fail gracefully)

        // CTIME(x) → CAST(x AS VARCHAR) of a timestamp — simplest: format_datetime(CAST x as TIMESTAMP, ...)
        if (opName.equalsIgnoreCase("CTIME") && call.operandCount() == 1) {
            writer.print("format_datetime(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP), 'EEE MMM dd HH:mm:ss yyyy')");
            return;
        }
        // CONVERT(x, type) → CAST(x AS type) — PPL compatibility
        if (opName.equalsIgnoreCase("CONVERT") && call.operandCount() == 2) {
            writer.print("CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }
        // ADDDATE(date, days) → date_add('day', days, CAST(date AS TIMESTAMP))
        if (opName.equalsIgnoreCase("ADDDATE") && call.operandCount() == 2) {
            writer.print("date_add('day', ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(", CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP))");
            return;
        }

        // TO_DAYS(date) → day of rata die; Trino: date_diff('day', DATE '0000-01-01', date)
        if (opName.equalsIgnoreCase("TO_DAYS") && call.operandCount() == 1) {
            writer.keyword("date_diff");
            SqlWriter.Frame frame = writer.startList("(", ")");
            writer.literal("'day'");
            writer.sep(",");
            writer.literal("DATE '0000-01-01'");
            writer.sep(",");
            writer.keyword("CAST");
            SqlWriter.Frame castFrame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.keyword("AS");
            writer.keyword("DATE");
            writer.endList(castFrame);
            writer.endList(frame);
            return;
        }
        // CIDRMATCH(ip, cidr) → contains(..., cast(ip as ipaddress))
        // simplest: regex check — but Trino has contains for IPADDRESS type only.
        // Defer: fall through (still generates function-not-found) — unknown enough to need proper UDF.
        // ASCII(x) → codepoint(cast(x as varchar(1))) — but simpler: Trino has no ASCII, use codepoint
        if (opName.equalsIgnoreCase("ASCII") && call.operandCount() == 1) {
            writer.keyword("codepoint");
            SqlWriter.Frame frame = writer.startList("(", ")");
            writer.keyword("CAST");
            SqlWriter.Frame castFrame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.keyword("AS");
            writer.keyword("VARCHAR(1)");
            writer.endList(castFrame);
            writer.endList(frame);
            return;
        }
        // MONTHNAME(x) → format_datetime(cast(x as timestamp), 'MMMM')
        if (opName.equalsIgnoreCase("MONTHNAME") && call.operandCount() == 1) {
            writer.keyword("format_datetime");
            SqlWriter.Frame frame = writer.startList("(", ")");
            writer.keyword("CAST");
            SqlWriter.Frame castFrame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.keyword("AS");
            writer.keyword("TIMESTAMP");
            writer.endList(castFrame);
            writer.sep(",");
            writer.literal("'MMMM'");
            writer.endList(frame);
            return;
        }

        // DATE_SUB(date, interval) → date_add('day', -interval, date)
        // Note: Trino has date_add(unit, value, timestamp)
        // This rewrite assumes DATE_SUB is used as DATE_SUB(date, days)
        // More complex interval handling may be needed based on actual usage
        if (opName.equalsIgnoreCase("DATE_SUB") && call.operandCount() == 2) {
            writer.keyword("date_add");
            SqlWriter.Frame frame = writer.startList("(", ")");
            writer.literal("'day'");
            writer.sep(",");
            writer.print("-");
            call.operand(1).unparse(writer, 0, 0);
            writer.sep(",");
            call.operand(0).unparse(writer, 0, 0);
            writer.endList(frame);
            return;
        }

        super.unparseCall(writer, call, leftPrec, rightPrec);
    }

    /** Helper: write "funcName(arg0, arg1, ...)" using call's existing operands. */
    private static void unparseFunctionLike(SqlWriter writer, String funcName, SqlCall call) {
        writer.keyword(funcName);
        SqlWriter.Frame frame = writer.startList("(", ")");
        for (int i = 0; i < call.operandCount(); i++) {
            if (i > 0) writer.sep(",");
            call.operand(i).unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }
}
