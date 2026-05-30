/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.type.ExprDateType;
import org.opensearch.sql.calcite.type.ExprIPType;
import org.opensearch.sql.calcite.type.ExprTimeStampType;
import org.opensearch.sql.calcite.type.ExprTimeType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/**
 * Custom Spark SQL dialect that extends Calcite's SparkSqlDialect to handle OpenSearch-specific
 * function translations. This dialect ensures that functions are translated to their correct Spark
 * SQL equivalents.
 */
public class OpenSearchSparkSqlDialect extends SparkSqlDialect {

  /** Singleton instance of the OpenSearch Spark SQL dialect. */
  public static final OpenSearchSparkSqlDialect DEFAULT = new OpenSearchSparkSqlDialect();

  private static final Map<String, String> CALCITE_TO_SPARK_MAPPING =
      ImmutableMap.of(
          "ARG_MIN", "MIN_BY",
          "ARG_MAX", "MAX_BY");

  private static final Map<String, String> CALL_SEPARATOR = ImmutableMap.of();

  private OpenSearchSparkSqlDialect() {
    super(DEFAULT_CONTEXT);
  }

  /**
   * Disable {@link SqlConformance#isSortByOrdinal()} so {@link
   * org.apache.calcite.rel.rel2sql.RelToSqlConverter} emits the full sort expression in {@code
   * ORDER BY} instead of an ordinal numeric literal.
   *
   * <p>When the PPL plan is shaped as {@code Project(narrow) on Sort($N) on
   * Project(wide_with_cast)} (common after {@code | sort cast_expr | fields x}), Calcite emits
   * {@code SELECT narrow FROM (SELECT wide_with_cast ... ORDER BY 13) t0}. The validator drops the
   * inner {@code ORDER BY} because it is meaningless on a derived table without {@code LIMIT}, and
   * the rebuilt plan loses the Sort entirely. Returning the full expression ({@code ORDER BY CAST(x
   * AS DOUBLE)}) avoids the {@code hasSortByOrdinal} check in {@code
   * SqlImplementor.needNewSubQuery} that triggers the subquery wrap, so the outer {@code Project}
   * merges into the same {@code SELECT} and the {@code ORDER BY} stays at the outermost level:
   * {@code SELECT narrow FROM ... ORDER BY CAST(...)}.
   */
  @Override
  public SqlConformance getConformance() {
    return new SqlDelegatingConformance(super.getConformance()) {
      @Override
      public boolean isSortByOrdinal() {
        return false;
      }
    };
  }

  /**
   * Spark dialect's default cast spec emits "STRING" for VARCHAR/CHAR. The SqlNodePipeline parser
   * can't parse {@code CAST(... AS STRING)}, so emit the SQL-standard type names instead. Other
   * type mappings continue to defer to the Spark dialect.
   *
   * <p>For {@link SqlTypeName#REAL} the default {@code SqlDialect.getCastSpec} emits {@code FLOAT},
   * which Babel (Spark) parses back as {@code DOUBLE}. PPL needs to preserve single precision
   * through the round-trip, so emit the Calcite-default {@code REAL} keyword which Babel maps back
   * to {@code SqlTypeName.REAL}.
   */
  @Override
  public SqlNode getCastSpec(RelDataType type) {
    SqlTypeName name = type.getSqlTypeName();
    // Emit a SqlUserDefinedTypeNameSpec for PPL UDTs so the SqlValidator round-trip can resolve
    // the type back to its original UDT class via the catalog (registered in QueryService). Stock
    // SqlTypeUtil.convertTypeToSpec rejects SqlTypeName.OTHER and falls through to the inner
    // BasicSqlType (e.g. VARCHAR for date-like UDTs), which loses UDT identity on re-parse.
    if (type instanceof AbstractExprRelDataType<?>) {
      ExprUDT udtName = pplUdtName(type);
      if (udtName != null) {
        return new SqlDataTypeSpec(
                new SqlUserDefinedTypeNameSpec(
                    new SqlIdentifier(udtName.name(), SqlParserPos.ZERO), SqlParserPos.ZERO),
                SqlParserPos.ZERO)
            .withNullable(type.isNullable());
      }
    }
    if (name == SqlTypeName.VARCHAR
        || name == SqlTypeName.CHAR
        || name == SqlTypeName.REAL
        || name == SqlTypeName.MAP
        || name == SqlTypeName.ARRAY) {
      // Spark's getCastSpec emits angle-bracket syntax (`MAP<K,V>`, `ARRAY<T>`) which the Babel
      // parser doesn't accept. Calcite default uses standard SQL `MAP`/`ARRAY` type names that
      // round-trip cleanly. Note: emitting bare `MAP`/`ARRAY` drops the parameter types — this
      // is acceptable because validator resolves the cast via the operand's typed expression.
      return org.apache.calcite.sql.dialect.CalciteSqlDialect.DEFAULT.getCastSpec(type);
    }
    if (name == SqlTypeName.ANY) {
      // SqlTypeUtil.convertTypeToSpec (which RelToSqlConverter.castNullType invokes via
      // SqlDialect.getCastSpec) rejects SqlTypeName.ANY outright. ANY appears in column types
      // when a Union merges two indices with conflicting field types (Calcite's leastRestrictive
      // collapses the column to ANY) and one branch contributes a NULL literal that
      // RelToSqlConverter wraps in CAST(NULL AS T). Emit a permissive type spec the Babel parser
      // accepts; the validator's schema for the union projection will re-anchor the column type.
      return new SqlDataTypeSpec(
              new SqlBasicTypeNameSpec(SqlTypeName.OTHER, -1, -1, null, SqlParserPos.ZERO),
              SqlParserPos.ZERO)
          .withNullable(type.isNullable());
    }
    return super.getCastSpec(type);
  }

  private static ExprUDT pplUdtName(RelDataType type) {
    if (type instanceof ExprDateType) return ExprUDT.EXPR_DATE;
    if (type instanceof ExprTimeType) return ExprUDT.EXPR_TIME;
    if (type instanceof ExprTimeStampType) return ExprUDT.EXPR_TIMESTAMP;
    if (type instanceof ExprIPType) return ExprUDT.EXPR_IP;
    return null;
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    String operatorName = call.getOperator().getName();

    if (CALCITE_TO_SPARK_MAPPING.containsKey(operatorName)) {
      unparseFunction(
          writer,
          call,
          CALCITE_TO_SPARK_MAPPING.get(operatorName),
          leftPrec,
          rightPrec,
          CALL_SEPARATOR.getOrDefault(operatorName, ","));
      return;
    }

    // SparkSqlDialect rewrites POSITION as a comma-separated function call
    // (POSITION(a, b[, c])) and TRIM as LTRIM/RTRIM/TRIM(arg). Both of those
    // forms break the SqlNodePipeline round-trip:
    //  - POSITION(a, b[, c]) is rejected by the Babel parser (which only
    //    accepts the standard POSITION(a IN b [FROM c])).
    //  - LTRIM(arg)/RTRIM(arg) are accepted by the parser but unresolved by
    //    the validator because PPL only registers the canonical TRIM operator.
    // Bypass SparkSqlDialect's rewrite for these kinds and let the operator's
    // own unparse emit the standard SQL form, which round-trips cleanly.
    SqlKind kind = call.getKind();
    if (kind == SqlKind.POSITION || kind == SqlKind.TRIM) {
      call.getOperator().unparse(writer, call, leftPrec, rightPrec);
      return;
    }

    super.unparseCall(writer, call, leftPrec, rightPrec);
  }

  private void unparseFunction(
      SqlWriter writer,
      SqlCall call,
      String functionName,
      int leftPrec,
      int rightPrec,
      String separator) {
    writer.print(functionName);
    final SqlWriter.Frame frame = writer.startList("(", ")");
    for (int i = 0; i < call.operandCount(); i++) {
      if (i > 0) {
        writer.sep(separator);
      }
      call.operand(i).unparse(writer, 0, rightPrec);
    }
    writer.endList(frame);
  }
}
