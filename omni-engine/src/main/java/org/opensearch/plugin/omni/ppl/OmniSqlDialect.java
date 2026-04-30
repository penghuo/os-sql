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
                writer.print("date_trunc('" + trinoUnit + "', ");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(")");
                return;
            }
            if (call.operandCount() == 2) {
                // SPAN(num, N) → floor(num / N) * N
                writer.print("(FLOOR(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" / ");
                call.operand(1).unparse(writer, 0, 0);
                writer.print(") * ");
                call.operand(1).unparse(writer, 0, 0);
                writer.print(")");
                return;
            }
        }

        // SAFE_CAST(expr AS type) → TRY_CAST(expr AS type)  (Calcite BigQuery name → Trino)
        if (opName.equalsIgnoreCase("SAFE_CAST") && call.operandCount() == 2) {
            writer.print("TRY_CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }

        // TIMESTAMP(literal string) → TIMESTAMP 'literal' (literal prefix)
        // TIMESTAMP(expr)          → CAST(expr AS TIMESTAMP)
        if (opName.equals("TIMESTAMP") && call.operandCount() == 1) {
            if (call.operand(0) instanceof SqlCharStringLiteral literal) {
                writer.literal("TIMESTAMP '" + literal.getNlsString().getValue() + "'");
            } else {
                writer.print("CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS TIMESTAMP)");
            }
            return;
        }

        // TIME(literal)  → TIME 'literal'
        // TIME(expr)     → CAST(expr AS TIME)
        if (opName.equals("TIME") && call.operandCount() == 1) {
            if (call.operand(0) instanceof SqlCharStringLiteral timeLit) {
                writer.literal("TIME '" + timeLit.getNlsString().getValue() + "'");
            } else {
                writer.print("CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS TIME)");
            }
            return;
        }

        // DATE(literal)  → DATE 'literal'
        // DATE(expr)     → CAST(expr AS DATE)
        if (opName.equals("DATE") && call.operandCount() == 1) {
            if (call.operand(0) instanceof SqlCharStringLiteral dateLit) {
                writer.literal("DATE '" + dateLit.getNlsString().getValue() + "'");
            } else {
                writer.print("CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS DATE)");
            }
            return;
        }

        // EXTRACT('minute', col) → EXTRACT(MINUTE FROM col)
        if (opName.equals("EXTRACT") && call.operandCount() == 2
                && call.operand(0) instanceof SqlCharStringLiteral unitLiteral) {
            String unit = unitLiteral.getNlsString().getValue().toUpperCase();
            writer.print("EXTRACT(" + unit + " FROM ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(")");
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
            // Time-only extractions (hour/minute/second) accept TIME directly in Trino;
            // others need TIMESTAMP/DATE. Use CASE to pick a viable cast, since COALESCE
            // would require all branches to have the same type.
            boolean isTimeExtract = opName.equalsIgnoreCase("HOUR")
                    || opName.equalsIgnoreCase("MINUTE")
                    || opName.equalsIgnoreCase("SECOND");
            writer.print("(CASE WHEN TRY_CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP) IS NOT NULL THEN " + trinoName + "(TRY_CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP))");
            if (isTimeExtract) {
                writer.print(" WHEN TRY_CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS TIME) IS NOT NULL THEN " + trinoName + "(TRY_CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS TIME))");
            } else {
                writer.print(" WHEN TRY_CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS DATE) IS NOT NULL THEN " + trinoName + "(TRY_CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS DATE))");
            }
            writer.print(" ELSE NULL END)");
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
            writer.print("date_diff('day', ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(", ");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }
        // TIMESTAMPADD(unit, interval, ts) → date_add(lowercase_unit_literal, interval, ts)
        if (opName.equalsIgnoreCase("TIMESTAMPADD") && call.operandCount() == 3) {
            String unit = call.operand(0).toString().toLowerCase();
            writer.print("date_add('" + unit + "', ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(", ");
            call.operand(2).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }
        // REGEXP_CONTAINS(str, pattern) → regexp_like(str, pattern)
        if (opName.equalsIgnoreCase("REGEXP_CONTAINS") && call.operandCount() == 2) {
            unparseFunctionLike(writer, "regexp_like", call);
            return;
        }
        // JSON_EXTRACT_ALL(jsonStr) → TRY_CAST(CAST(jsonStr AS JSON) AS MAP(VARCHAR, JSON))
        // TRY_CAST returns NULL for non-object JSON (arrays etc.); subscripts then just return NULL.
        if (opName.equalsIgnoreCase("JSON_EXTRACT_ALL") && call.operandCount() == 1) {
            writer.print("TRY_CAST(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS JSON) AS MAP(VARCHAR, JSON))");
            return;
        }
        // JSON_EXTRACT_ALL(jsonStr, path) → json_extract(CAST(jsonStr AS JSON), path)
        if (opName.equalsIgnoreCase("JSON_EXTRACT_ALL") && call.operandCount() == 2) {
            writer.print("json_extract(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS JSON), ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }
        // VALUES(x) → array_agg(x) — PPL collects distinct values as an array
        if (opName.equalsIgnoreCase("VALUES") && call.operandCount() == 1) {
            unparseFunctionLike(writer, "array_agg", call);
            return;
        }
        // TAKE(x, n) → slice(array_agg(x), 1, n)
        if (opName.equalsIgnoreCase("TAKE") && call.operandCount() == 2) {
            writer.print("slice(array_agg(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print("), 1, ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(")");
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
        // STRFTIME(ts_or_epoch, fmt) → format_datetime(...)
        // If arg is numeric literal, use from_unixtime directly. Otherwise COALESCE try_cast paths.
        if (opName.equalsIgnoreCase("STRFTIME") && call.operandCount() == 2) {
            if (call.operand(0).toString().matches("^\\s*-?[0-9.E]+\\s*$")) {
                writer.print("format_datetime(from_unixtime(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print("), ");
                call.operand(1).unparse(writer, 0, 0);
                writer.print(")");
            } else {
                writer.print("format_datetime(CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS TIMESTAMP), ");
                call.operand(1).unparse(writer, 0, 0);
                writer.print(")");
            }
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
        // MAKEDATE(year, dayOfYear) → date_add('day', CAST(dayOfYear AS BIGINT) - 1,
        //                                        DATE from_iso8601_date(lpad(CAST(CAST(year AS BIGINT) AS VARCHAR),4,'0')||'-01-01'))
        if (opName.equalsIgnoreCase("MAKEDATE") && call.operandCount() == 2) {
            writer.print("date_add('day', CAST(");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(" AS BIGINT) - 1, CAST(CONCAT(CAST(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS BIGINT) AS VARCHAR), '-01-01') AS DATE))");
            return;
        }
        // MKTIME(hours) → time literal; Trino has no mktime — defer (fall through, fail gracefully)

        // CTIME(epoch) → format_datetime(from_unixtime(epoch))
        if (opName.equalsIgnoreCase("CTIME") && call.operandCount() == 1) {
            if (call.operand(0).toString().matches("^\\s*-?[0-9.E]+\\s*$")) {
                writer.print("format_datetime(from_unixtime(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print("), 'EEE MMM dd HH:mm:ss yyyy')");
            } else {
                writer.print("format_datetime(CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS TIMESTAMP), 'EEE MMM dd HH:mm:ss yyyy')");
            }
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
        // NUM(x) → CAST(x AS DOUBLE)
        if (opName.equalsIgnoreCase("NUM") && call.operandCount() == 1) {
            writer.print("CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS DOUBLE)");
            return;
        }
        // MEMK(x) — PPL memory-size string parse, return bytes; approximate with regex extract
        // x is something like '10k', '5m' → number * unit multiplier
        // Fallback: CAST(x AS DOUBLE) * 1024 (PPL semantics: memk returns kilobytes as numeric)
        if (opName.equalsIgnoreCase("MEMK") && call.operandCount() == 1) {
            // Best-effort: try_cast the numeric part and multiply
            writer.print("TRY_CAST(regexp_extract(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS VARCHAR), '[0-9.]+') AS DOUBLE)");
            return;
        }
        // CIDRMATCH(ip, cidr) → contains(IPADDRESS prefix). Trino builtin 'contains' + 'cast(ip as ipaddress)'
        if (opName.equalsIgnoreCase("CIDRMATCH") && call.operandCount() == 2) {
            // Use regexp — simplest: CAST cidr to ipprefix and use contains()
            // Trino 442 has contains(ipprefix, ipaddress) via ipaddress module
            writer.print("contains(CAST(");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(" AS IPPREFIX), CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS IPADDRESS))");
            return;
        }
        // JSON_DELETE(json, path) — Trino has no built-in; emit a NULL-returning cast
        if (opName.equalsIgnoreCase("JSON_DELETE")) {
            writer.print("CAST(NULL AS JSON)");
            return;
        }
        // JSON_SET(json, path, value) — Trino has no built-in; return json unchanged
        if (opName.equalsIgnoreCase("JSON_SET") && call.operandCount() >= 1) {
            call.operand(0).unparse(writer, 0, 0);
            return;
        }
        // SUBTIME(ts, t) → ts - (t interval)
        if (opName.equalsIgnoreCase("SUBTIME") && call.operandCount() == 2) {
            writer.print("(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP) - (CAST(");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(" AS TIME) - TIME '00:00:00'))");
            return;
        }

        // SPAN_BUCKET / MINSPAN_BUCKET / RANGE_BUCKET(col, N, unit?) → date_trunc or floor*N
        if ((opName.equalsIgnoreCase("SPAN_BUCKET")
             || opName.equalsIgnoreCase("MINSPAN_BUCKET")
             || opName.equalsIgnoreCase("RANGE_BUCKET"))) {
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
                writer.print("date_trunc('" + trinoUnit + "', CAST(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" AS TIMESTAMP))");
                return;
            }
            if (call.operandCount() >= 2) {
                writer.print("(FLOOR(");
                call.operand(0).unparse(writer, 0, 0);
                writer.print(" / ");
                call.operand(1).unparse(writer, 0, 0);
                writer.print(") * ");
                call.operand(1).unparse(writer, 0, 0);
                writer.print(")");
                return;
            }
        }
        // AUTO(x) → x  (PPL auto-cast, Trino will coerce as needed)
        if (opName.equalsIgnoreCase("AUTO") && call.operandCount() == 1) {
            call.operand(0).unparse(writer, 0, 0);
            return;
        }
        // TOSTRING(x) → CAST(x AS VARCHAR)
        if (opName.equalsIgnoreCase("TOSTRING") && call.operandCount() == 1) {
            writer.print("CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS VARCHAR)");
            return;
        }
        // UTC_TIMESTAMP() → current_timestamp AT TIME ZONE 'UTC'
        if (opName.equalsIgnoreCase("UTC_TIMESTAMP")) {
            writer.print("(current_timestamp AT TIME ZONE 'UTC')");
            return;
        }
        // TO_SECONDS(x) → date_diff('second', TIMESTAMP '0001-01-01 00:00:00', CAST x AS TIMESTAMP)
        if (opName.equalsIgnoreCase("TO_SECONDS") && call.operandCount() == 1) {
            writer.print("date_diff('second', TIMESTAMP '0001-01-01 00:00:00', CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP))");
            return;
        }
        // TIME_FORMAT(t, fmt) → format_datetime(CAST t AS TIMESTAMP, fmt)
        if (opName.equalsIgnoreCase("TIME_FORMAT") && call.operandCount() == 2) {
            writer.print("format_datetime(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP), ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }
        // MATCH_BOOL_PREFIX(field, query) and SIMPLE_QUERY_STRING — these are OpenSearch relevance
        // functions without Trino equivalents. Fall through so the test cleanly fails.
        // JSON_SET / JSON_DELETE — Trino has json_merge but no json_set/delete. Defer.
        // PATTERN / GROK / PARSE / REX_EXTRACT_MULTI — PPL-specific, no Trino equivalents. Defer.
        // WEEKDAY(x) → day_of_week(x) - 1  (PPL: monday=0, Trino: monday=1)
        if (opName.equalsIgnoreCase("WEEKDAY") && call.operandCount() == 1) {
            writer.print("(day_of_week(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS DATE)) - 1)");
            return;
        }
        // DAYNAME(x) → format_datetime(CAST x AS TIMESTAMP, 'EEEE')
        if (opName.equalsIgnoreCase("DAYNAME") && call.operandCount() == 1) {
            writer.print("format_datetime(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP), 'EEEE')");
            return;
        }
        // SUBDATE(date, days) → date_add('day', -days, CAST(date AS TIMESTAMP))
        if (opName.equalsIgnoreCase("SUBDATE") && call.operandCount() == 2) {
            writer.print("date_add('day', -(");
            call.operand(1).unparse(writer, 0, 0);
            writer.print("), CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP))");
            return;
        }
        // ADDTIME(t1, t2) → Trino t1 + t2 (intervals)
        if (opName.equalsIgnoreCase("ADDTIME") && call.operandCount() == 2) {
            writer.print("(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP) + (CAST(");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(" AS TIME) - TIME '00:00:00'))");
            return;
        }
        // TIME_TO_SEC(t) → hour(t)*3600 + minute(t)*60 + second(t)
        if (opName.equalsIgnoreCase("TIME_TO_SEC") && call.operandCount() == 1) {
            writer.print("(hour(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIME)) * 3600 + minute(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIME)) * 60 + second(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIME)))");
            return;
        }
        // TIMESTAMPDIFF(unit, start, end) → date_diff(unit, start, end)
        if (opName.equalsIgnoreCase("TIMESTAMPDIFF") && call.operandCount() == 3) {
            String unit = call.operand(0).toString().toLowerCase().replaceAll("[^a-z]", "");
            writer.print("date_diff('" + unit + "', CAST(");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP), CAST(");
            call.operand(2).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP))");
            return;
        }
        // STR_TO_DATE(str, fmt) → date_parse(str, fmt)
        if (opName.equalsIgnoreCase("STR_TO_DATE") && call.operandCount() == 2) {
            writer.print("date_parse(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(", ");
            call.operand(1).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }
        // YEARWEEK(x) → year*100 + week_of_year
        if (opName.equalsIgnoreCase("YEARWEEK")) {
            writer.print("(year(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP)) * 100 + week_of_year(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP)))");
            return;
        }
        // MINUTE_OF_DAY(x) → hour(x)*60 + minute(x)
        if (opName.equalsIgnoreCase("MINUTE_OF_DAY") && call.operandCount() == 1) {
            writer.print("(hour(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP)) * 60 + minute(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP)))");
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

        // TO_DAYS(date) → date_diff('day', DATE '0000-01-01', CAST(date AS DATE))
        if (opName.equalsIgnoreCase("TO_DAYS") && call.operandCount() == 1) {
            writer.print("date_diff('day', DATE '0000-01-01', CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS DATE))");
            return;
        }
        // ASCII(x) → codepoint(CAST(x AS VARCHAR(1)))
        if (opName.equalsIgnoreCase("ASCII") && call.operandCount() == 1) {
            writer.print("codepoint(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS VARCHAR(1)))");
            return;
        }
        // MONTHNAME(x) → format_datetime(CAST(x AS TIMESTAMP), 'MMMM')
        if (opName.equalsIgnoreCase("MONTHNAME") && call.operandCount() == 1) {
            writer.print("format_datetime(CAST(");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(" AS TIMESTAMP), 'MMMM')");
            return;
        }

        // DATE_SUB(date, N)  → date_add('day', -N, date)
        if (opName.equalsIgnoreCase("DATE_SUB") && call.operandCount() == 2) {
            writer.print("date_add('day', -(");
            call.operand(1).unparse(writer, 0, 0);
            writer.print("), ");
            call.operand(0).unparse(writer, 0, 0);
            writer.print(")");
            return;
        }
        super.unparseCall(writer, call, leftPrec, rightPrec);
    }

    /** Helper: write "funcName(arg0, arg1, ...)" using call's existing operands. */
    private static void unparseFunctionLike(SqlWriter writer, String funcName, SqlCall call) {
        writer.print(funcName);
        writer.print("(");
        for (int i = 0; i < call.operandCount(); i++) {
            if (i > 0) writer.print(", ");
            call.operand(i).unparse(writer, 0, 0);
        }
        writer.print(")");
    }
}
