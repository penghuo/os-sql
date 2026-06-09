/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Static helpers for converting PPL expression-AST primitives to SqlNode. Stateless utilities only;
 * stateful expression conversion lives on {@link PPLToSqlNodeVisitor} for now and migrates here
 * incrementally.
 */
final class ExpressionConverter {

  private static final SqlParserPos POS = SqlParserPos.ZERO;
  private static final SqlParserPos QPOS = SqlParserPos.ZERO.withQuoting(true);

  private ExpressionConverter() {}

  /**
   * Convert a PPL {@link Literal} to a Calcite {@link SqlNode}. Mirrors the type-by-type emission
   * shape required by downstream validator and pushdown rules:
   *
   * <ul>
   *   <li>BOOLEAN → {@code SqlLiteral.createBoolean}.
   *   <li>INTEGER / LONG / SHORT → exact numeric.
   *   <li>DECIMAL → exact numeric (plain string, no scientific notation).
   *   <li>DOUBLE → approximate numeric. PPL ASTs sometimes carry doubles without scientific
   *       notation; Calcite's {@code createApproxNumeric} requires an {@code e}/{@code E} exponent,
   *       so coerce.
   *   <li>FLOAT → cast approximate numeric to FLOAT so the validator preserves PPL's {@code 0.06f}
   *       typing rather than widening to DOUBLE during arithmetic.
   *   <li>STRING → CHAR string literal.
   * </ul>
   */
  static SqlNode literalToSqlNode(Literal lit) {
    Object v = lit.getValue();
    if (v == null) return SqlLiteral.createNull(POS);
    return switch (lit.getType()) {
      case BOOLEAN -> SqlLiteral.createBoolean((Boolean) v, POS);
      case INTEGER, LONG, SHORT -> SqlLiteral.createExactNumeric(v.toString(), POS);
      case DECIMAL -> {
        BigDecimal bd = (v instanceof BigDecimal b) ? b : new BigDecimal(v.toString());
        yield SqlLiteral.createExactNumeric(bd.toPlainString(), POS);
      }
      case DOUBLE -> {
        String s = v.toString();
        if (!s.contains("e") && !s.contains("E")) {
          s = s + "E0";
        }
        yield SqlLiteral.createApproxNumeric(s, POS);
      }
      case FLOAT -> {
        String s = v.toString();
        if (!s.contains("e") && !s.contains("E")) {
          s = s + "E0";
        }
        SqlNode approx = SqlLiteral.createApproxNumeric(s, POS);
        SqlDataTypeSpec floatSpec =
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.FLOAT, POS), POS);
        yield new SqlBasicCall(SqlStdOperatorTable.CAST, List.of(approx, floatSpec), POS);
      }
      case STRING -> SqlLiteral.createCharString(v.toString(), POS);
      default ->
          throw new UnsupportedOperationException(
              "Literal type not yet supported in ON-clause: " + lit.getType());
    };
  }

  /** Cast a string literal to VARCHAR. */
  static SqlNode castStringToVarchar(String s) {
    SqlDataTypeSpec spec =
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, POS), POS);
    return new SqlBasicCall(
        SqlLibraryOperators.SAFE_CAST, List.of(SqlLiteral.createCharString(s, POS), spec), POS);
  }

  /** {@code CASE WHEN <cond> THEN 1 ELSE 0 END} — used for streamstats reset flags. */
  static SqlNode caseFlagOneZero(SqlNode cond) {
    SqlNodeList whens = new SqlNodeList(POS);
    whens.add(cond);
    SqlNodeList thens = new SqlNodeList(POS);
    thens.add(SqlLiteral.createExactNumeric("1", POS));
    return new SqlCase(POS, null, whens, thens, SqlLiteral.createExactNumeric("0", POS));
  }

  /** True when {@code e} is a STRING-typed Literal. */
  static boolean isStringLiteral(UnresolvedExpression e) {
    return e instanceof Literal lit && lit.getType() == DataType.STRING;
  }

  /**
   * Extract a boolean value from a Literal that's either typed BOOLEAN or a STRING containing
   * "TRUE"/"FALSE" (case-insensitive). Returns null otherwise. PPL allows both forms when comparing
   * against a boolean field (e.g. {@code where male = 'TRUE'} and {@code where male = TRUE}).
   */
  static Boolean extractBoolLiteral(UnresolvedExpression e) {
    if (!(e instanceof Literal lit)) return null;
    if (lit.getType() == DataType.BOOLEAN) {
      return (Boolean) lit.getValue();
    }
    if (lit.getType() == DataType.STRING) {
      String s = String.valueOf(lit.getValue());
      if ("TRUE".equalsIgnoreCase(s)) return Boolean.TRUE;
      if ("FALSE".equalsIgnoreCase(s)) return Boolean.FALSE;
    }
    return null;
  }

  /**
   * Map a comparison operator string (=/!=/&gt;/&gt;=/&lt;/&lt;=) to its IP-aware PPL comparator.
   * Mirrors v2's PPLFuncImpTable.registerOperator(GREATER, GREATER_IP, GT) dispatch. Returns null
   * when {@code op} is not a recognized comparison operator.
   */
  static SqlOperator ipComparisonOperator(String op) {
    return switch (op) {
      case "=" -> PPLBuiltinOperators.EQUALS_IP;
      case "!=", "<>" -> PPLBuiltinOperators.NOT_EQUALS_IP;
      case ">" -> PPLBuiltinOperators.GREATER_IP;
      case ">=" -> PPLBuiltinOperators.GTE_IP;
      case "<" -> PPLBuiltinOperators.LESS_IP;
      case "<=" -> PPLBuiltinOperators.LTE_IP;
      default -> null;
    };
  }

  /** Map a comparison operator string to its standard Calcite operator. */
  static SqlOperator comparisonOperator(String op) {
    return switch (op.toLowerCase(Locale.ROOT)) {
      case "=" -> SqlStdOperatorTable.EQUALS;
      case "!=", "<>" -> SqlStdOperatorTable.NOT_EQUALS;
      case "<" -> SqlStdOperatorTable.LESS_THAN;
      case "<=" -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case ">" -> SqlStdOperatorTable.GREATER_THAN;
      case ">=" -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
      case "like" -> SqlStdOperatorTable.LIKE;
      case "not like", "not_like" -> SqlStdOperatorTable.NOT_LIKE;
      case "ilike" -> SqlLibraryOperators.ILIKE;
      case "not ilike", "not_ilike" -> SqlLibraryOperators.NOT_ILIKE;
      case "regexp" -> SqlLibraryOperators.REGEXP_CONTAINS;
      default ->
          throw new UnsupportedOperationException("Comparison operator not supported: " + op);
    };
  }

  /** Map a UDT root name (timestamp/date/time/ip) to the corresponding PPL constructor UDF. */
  static SqlOperator udtConstructorOpForRoot(String root) {
    return switch (root) {
      case "timestamp" -> PPLBuiltinOperators.TIMESTAMP;
      case "date" -> PPLBuiltinOperators.DATE;
      case "time" -> PPLBuiltinOperators.TIME;
      case "ip" -> PPLBuiltinOperators.IP;
      default -> null;
    };
  }

  /**
   * Build a SqlIdentifier whose every part is wrapped in quotes (Calcite emits backticks/double
   * quotes around them). PPL needs this for dotted column names that must resolve as a single
   * literal column name rather than as multi-part qualified path resolution.
   */
  static SqlIdentifier quotedIdentifier(List<String> parts) {
    List<SqlParserPos> positions = new ArrayList<>(parts.size());
    for (int i = 0; i < parts.size(); i++) {
      positions.add(QPOS);
    }
    return new SqlIdentifier(parts, null, POS, positions);
  }

  /**
   * Best-effort static check: when {@code e} is statically a DATE/TIME/TIMESTAMP UDT, return its
   * lowercase name; otherwise null. True for {@code Cast(... AS DATE/TIME/TIMESTAMP)} and for
   * {@code Function("date"/"time"/"timestamp", ...)} calls. Used by Compare to coerce cross-type
   * datetime operands to TIMESTAMP. Without an oracle we can't infer types from column refs.
   */
  static String staticDateTimeUdtName(UnresolvedExpression e) {
    if (e instanceof Cast c) {
      return switch (c.getDataType()) {
        case DATE -> "date";
        case TIME -> "time";
        case TIMESTAMP -> "timestamp";
        default -> null;
      };
    }
    if (e instanceof Function fn) {
      String name = fn.getFuncName() == null ? "" : fn.getFuncName().toLowerCase(Locale.ROOT);
      return switch (name) {
        case "date", "time", "timestamp" -> name;
        default -> null;
      };
    }
    return null;
  }

  /**
   * Return the PPL primitive {@link DataType} of an expression when statically inferable from the
   * AST plus the per-translation {@link Frame} (column-type maps for catalog columns and
   * eval-alias-types for aliased eval expressions). Returns null when the type cannot be determined
   * without a validator probe.
   *
   * <p>Used by {@code expr()} for boolean-comparison simplification and similar cast-shape
   * decisions.
   */
  static DataType staticTypeOf(UnresolvedExpression e, Frame frame) {
    if (e == null) return null;
    if (e instanceof Literal lit) return lit.getType();
    if (e instanceof Cast c) return c.getDataType();
    if (e instanceof Function fn) {
      String name = fn.getFuncName() == null ? "" : fn.getFuncName().toLowerCase(Locale.ROOT);
      switch (name) {
        case "date":
        case "current_date":
        case "curdate":
          return DataType.DATE;
        case "time":
        case "current_time":
        case "curtime":
          return DataType.TIME;
        case "timestamp":
        case "now":
        case "current_timestamp":
        case "localtime":
        case "localtimestamp":
          return DataType.TIMESTAMP;
        case "ip":
          return DataType.IP;
        default:
          return null;
      }
    }
    if (e instanceof QualifiedName qn && qn.getParts().size() == 1 && frame != null) {
      String name = qn.toString();
      if (frame.evalAliasTypes.containsKey(name)) {
        return frame.evalAliasTypes.get(name);
      }
      if (frame.columnPrimitiveType.containsKey(name)) {
        return frame.columnPrimitiveType.get(name);
      }
    }
    if (e instanceof Field f && f.getField() instanceof QualifiedName qn) {
      if (qn.getParts().size() == 1 && frame != null) {
        String name = qn.toString();
        if (frame.evalAliasTypes.containsKey(name)) {
          return frame.evalAliasTypes.get(name);
        }
        if (frame.columnPrimitiveType.containsKey(name)) {
          return frame.columnPrimitiveType.get(name);
        }
      }
    }
    return null;
  }
}
