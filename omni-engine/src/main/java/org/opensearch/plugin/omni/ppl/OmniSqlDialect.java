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

        // TIMESTAMP('2023-01-01 00:00:00') → TIMESTAMP '2023-01-01 00:00:00'
        if (opName.equals("TIMESTAMP") && call.operandCount() == 1
                && call.operand(0) instanceof SqlCharStringLiteral literal) {
            writer.literal("TIMESTAMP '" + literal.getNlsString().getValue() + "'");
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

        // DAYOFWEEK(x) → day_of_week(x)
        if (opName.equalsIgnoreCase("DAYOFWEEK") && call.operandCount() == 1) {
            writer.keyword("day_of_week");
            SqlWriter.Frame frame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.endList(frame);
            return;
        }

        // DAYOFMONTH(x) → day(x) [Trino's alias for day_of_month]
        if (opName.equalsIgnoreCase("DAYOFMONTH") && call.operandCount() == 1) {
            writer.keyword("day");
            SqlWriter.Frame frame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.endList(frame);
            return;
        }

        // DAYOFYEAR(x) → day_of_year(x)
        if (opName.equalsIgnoreCase("DAYOFYEAR") && call.operandCount() == 1) {
            writer.keyword("day_of_year");
            SqlWriter.Frame frame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.endList(frame);
            return;
        }

        // LAST_DAY(x) → last_day_of_month(x)
        if (opName.equalsIgnoreCase("LAST_DAY") && call.operandCount() == 1) {
            writer.keyword("last_day_of_month");
            SqlWriter.Frame frame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
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
}
