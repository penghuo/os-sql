/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.LambdaFunction;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Sibling expression visitor for {@link PPLToSqlNodeVisitor}. Mirrors the v2 split between
 * CalciteRelNodeVisitor (relational) and CalciteRexNodeVisitor (expressions). Holds a back-ref to
 * the relational visitor for shared state ({@link PPLToSqlNodeVisitor#exprFrame}, plan-subquery
 * dispatch via {@code outer.expr}) and grows incrementally as more methods migrate from the
 * relational visitor.
 */
final class SqlExpressionVisitor {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  private static final Set<String> FUNC_NAMES_KEEP_CHAR_LITERAL =
      Set.of(
          "like",
          "ilike",
          "not_like",
          "not_ilike",
          "rlike",
          "regexp",
          "regexp_match",
          "regexp_replace",
          "regexp_extract",
          "regexp_extract_all",
          "regex",
          "replace",
          "fillnull",
          "transpose",
          "array",
          "mvappend",
          "mvjoin",
          "array_join");

  private final PPLToSqlNodeVisitor outer;

  SqlExpressionVisitor(PPLToSqlNodeVisitor outer) {
    this.outer = outer;
  }

  SqlNode funcExpr(org.opensearch.sql.ast.expression.Function fn) {
    String fnLower = fn.getFuncName().toLowerCase(Locale.ROOT);
    if (("mvmap".equals(fnLower) || "transform".equals(fnLower))
        && fn.getFuncArgs().size() >= 2
        && fn.getFuncArgs().get(1) instanceof LambdaFunction lf) {
      Set<String> declared = new LinkedHashSet<>();
      for (QualifiedName qn : lf.getFuncArgs()) declared.add(qn.toString());
      LinkedHashMap<String, QualifiedName> captured = new LinkedHashMap<>();
      collectCapturedRefs(lf.getFunction(), declared, captured);
      if (!captured.isEmpty()) {
        List<QualifiedName> newLambdaArgs = new ArrayList<>(lf.getFuncArgs());
        newLambdaArgs.addAll(captured.values());
        LambdaFunction extendedLambda = new LambdaFunction(lf.getFunction(), newLambdaArgs);
        List<UnresolvedExpression> newOuterArgs = new ArrayList<>();
        newOuterArgs.add(fn.getFuncArgs().get(0));
        newOuterArgs.add(extendedLambda);
        for (QualifiedName qn : captured.values()) newOuterArgs.add(qn);
        return funcExpr(
            new org.opensearch.sql.ast.expression.Function(fn.getFuncName(), newOuterArgs));
      }
    }
    boolean isCoalesce = "coalesce".equals(fnLower) || "ifnull".equals(fnLower);
    List<SqlNode> args = new ArrayList<>(fn.getFuncArgs().size());
    for (UnresolvedExpression a : fn.getFuncArgs()) {
      if (isCoalesce && (isNullLiteralRef(a) || isUnknownFieldRef(a))) {
        args.add(SqlLiteral.createNull(POS));
      } else {
        args.add(outer.expr(a));
      }
    }
    if (!FUNC_NAMES_KEEP_CHAR_LITERAL.contains(fnLower)) {
      SqlDataTypeSpec varcharSpec =
          new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, POS), POS);
      for (int i = 0; i < args.size(); i++) {
        UnresolvedExpression a = fn.getFuncArgs().get(i);
        if (a instanceof Literal lit
            && lit.getType() == DataType.STRING
            && lit.getValue() != null) {
          args.set(
              i,
              new SqlBasicCall(SqlStdOperatorTable.CAST, List.of(args.get(i), varcharSpec), POS));
        }
      }
    }
    if (!args.isEmpty()
        && fn.getFuncArgs().get(0) instanceof Literal lit0
        && lit0.getType() == DataType.STRING
        && (fnLower.equals("week")
            || fnLower.equals("weekofyear")
            || fnLower.equals("week_of_year")
            || fnLower.equals("yearweek"))) {
      args.set(0, new SqlBasicCall(PPLBuiltinOperators.TIMESTAMP, List.of(args.get(0)), POS));
    }
    SqlOperator op = arithmeticOperator(fn.getFuncName());
    if (op != null) {
      if (op == SqlStdOperatorTable.PLUS && hasStringOperand(fn.getFuncArgs())) {
        return new SqlBasicCall(SqlStdOperatorTable.CONCAT, args, POS);
      }
      if ((op == SqlStdOperatorTable.PLUS || op == SqlStdOperatorTable.MINUS)
          && fn.getFuncArgs().size() == 2
          && fn.getFuncArgs().get(1) instanceof org.opensearch.sql.ast.expression.Interval) {
        SqlOperator dateOp =
            op == SqlStdOperatorTable.PLUS
                ? PPLBuiltinOperators.DATE_ADD
                : PPLBuiltinOperators.DATE_SUB;
        return new SqlBasicCall(dateOp, args, POS);
      }
      return new SqlBasicCall(op, args, POS);
    }
    if ("typeof".equals(fnLower) && fn.getFuncArgs().size() == 1) {
      String legacyName = pplLegacyTypeName(fn.getFuncArgs().get(0), outer.exprFrame);
      if (legacyName != null) {
        return SqlLiteral.createCharString(legacyName, POS);
      }
    }
    if ("json_valid".equals(fnLower) && args.size() == 1) {
      return new SqlBasicCall(SqlStdOperatorTable.IS_JSON_VALUE, args, POS);
    }
    if ("json_keys".equals(fnLower) && args.size() == 1) {
      return new SqlBasicCall(PPLBuiltinOperators.JSON_KEYS, args, POS);
    }
    if ("json_extract".equals(fnLower)) {
      return new SqlBasicCall(PPLBuiltinOperators.JSON_EXTRACT, args, POS);
    }
    if ("json_array_length".equals(fnLower) && args.size() == 1) {
      return new SqlBasicCall(PPLBuiltinOperators.JSON_ARRAY_LENGTH, args, POS);
    }
    if ("json_set".equals(fnLower)) {
      return new SqlBasicCall(PPLBuiltinOperators.JSON_SET, args, POS);
    }
    if ("json_append".equals(fnLower)) {
      return new SqlBasicCall(PPLBuiltinOperators.JSON_APPEND, args, POS);
    }
    if ("json_extend".equals(fnLower)) {
      return new SqlBasicCall(PPLBuiltinOperators.JSON_EXTEND, args, POS);
    }
    if ("json_delete".equals(fnLower)) {
      return new SqlBasicCall(PPLBuiltinOperators.JSON_DELETE, args, POS);
    }
    if ("json_object".equals(fnLower) || "json_array".equals(fnLower)) {
      List<SqlNode> jsonArgs = new ArrayList<>();
      jsonArgs.add(
          SqlLiteral.createSymbol(
              org.apache.calcite.sql.SqlJsonConstructorNullClause.NULL_ON_NULL, POS));
      List<UnresolvedExpression> userArgs = fn.getFuncArgs();
      boolean isObject = "json_object".equals(fnLower);
      for (int i = 0; i < args.size(); i++) {
        SqlNode a = args.get(i);
        boolean isValuePos = !isObject || (i % 2) == 1;
        boolean nestedJsonCtor =
            i < userArgs.size()
                && userArgs.get(i) instanceof org.opensearch.sql.ast.expression.Function nfn
                && (nfn.getFuncName().equalsIgnoreCase("json_object")
                    || nfn.getFuncName().equalsIgnoreCase("json_array"));
        if (isValuePos && nestedJsonCtor) {
          a =
              new SqlBasicCall(
                  SqlStdOperatorTable.CAST,
                  List.of(
                      a,
                      new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, POS), POS)),
                  POS);
        }
        jsonArgs.add(a);
      }
      return new SqlBasicCall(
          isObject ? SqlStdOperatorTable.JSON_OBJECT : SqlStdOperatorTable.JSON_ARRAY,
          jsonArgs,
          POS);
    }
    String name = fn.getFuncName().toLowerCase(Locale.ROOT);
    SqlOperator opOverride =
        switch (name) {
          case "isnull", "is_null", "is null" -> SqlStdOperatorTable.IS_NULL;
          case "isnotnull", "is_not_null", "is not null", "ispresent" ->
              SqlStdOperatorTable.IS_NOT_NULL;
          case "like" -> SqlStdOperatorTable.LIKE;
          case "not_like", "not like" -> SqlStdOperatorTable.NOT_LIKE;
          case "ilike" -> SqlLibraryOperators.ILIKE;
          case "not_ilike", "not ilike" -> SqlLibraryOperators.NOT_ILIKE;
          case "coalesce" -> SqlStdOperatorTable.COALESCE;
          case "nullif" -> SqlStdOperatorTable.NULLIF;
          case "abs" -> SqlStdOperatorTable.ABS;
          case "ceil", "ceiling" -> SqlStdOperatorTable.CEIL;
          case "floor" -> SqlStdOperatorTable.FLOOR;
          case "exp" -> SqlStdOperatorTable.EXP;
          case "ln" -> SqlStdOperatorTable.LN;
          case "log10" -> SqlStdOperatorTable.LOG10;
          case "sqrt" -> SqlStdOperatorTable.SQRT;
          case "round" -> SqlStdOperatorTable.ROUND;
          case "mod" -> PPLBuiltinOperators.MOD;
          case "pow", "power" -> SqlStdOperatorTable.POWER;
          case "add" -> SqlStdOperatorTable.PLUS;
          case "subtract" -> SqlStdOperatorTable.MINUS;
          case "multiply" -> SqlStdOperatorTable.MULTIPLY;
          case "divide" -> PPLBuiltinOperators.DIVIDE;
          case "modulus" -> PPLBuiltinOperators.MOD;
          case "atan2" -> SqlStdOperatorTable.ATAN2;
          case "sign", "signum" -> SqlStdOperatorTable.SIGN;
          case "conv" -> PPLBuiltinOperators.CONV;
          case "year" -> PPLBuiltinOperators.YEAR;
          case "quarter" -> PPLBuiltinOperators.QUARTER;
          case "month", "month_of_year" -> PPLBuiltinOperators.MONTH;
          case "dayofyear", "day_of_year" -> PPLBuiltinOperators.DAY_OF_YEAR;
          case "dayofmonth", "day_of_month", "day" -> PPLBuiltinOperators.DAY;
          case "dayofweek", "day_of_week" -> PPLBuiltinOperators.DAY_OF_WEEK;
          case "hour", "hour_of_day" -> PPLBuiltinOperators.HOUR;
          case "minute", "minute_of_hour" -> PPLBuiltinOperators.MINUTE;
          case "second", "second_of_minute" -> PPLBuiltinOperators.SECOND;
          case "week", "week_of_year", "weekofyear" -> PPLBuiltinOperators.WEEK;
          case "weekday" -> PPLBuiltinOperators.WEEKDAY;
          case "yearweek" -> PPLBuiltinOperators.YEARWEEK;
          case "timestampadd" -> PPLBuiltinOperators.TIMESTAMPADD;
          case "timestampdiff" -> PPLBuiltinOperators.TIMESTAMPDIFF;
          case "last_day" -> PPLBuiltinOperators.LAST_DAY;
          case "extract" -> PPLBuiltinOperators.EXTRACT;
          case "date_add" -> PPLBuiltinOperators.DATE_ADD;
          case "adddate" -> PPLBuiltinOperators.ADDDATE;
          case "date_sub" -> PPLBuiltinOperators.DATE_SUB;
          case "subdate" -> PPLBuiltinOperators.SUBDATE;
          case "addtime" -> PPLBuiltinOperators.ADDTIME;
          case "subtime" -> PPLBuiltinOperators.SUBTIME;
          case "datediff" -> PPLBuiltinOperators.DATEDIFF;
          case "timediff" -> PPLBuiltinOperators.TIMEDIFF;
          case "lower", "lcase" -> SqlStdOperatorTable.LOWER;
          case "upper", "ucase" -> SqlStdOperatorTable.UPPER;
          case "substring", "substr" -> SqlStdOperatorTable.SUBSTRING;
          case "concat" -> SqlLibraryOperators.CONCAT_FUNCTION;
          case "length", "char_length", "character_length" -> SqlLibraryOperators.LENGTH;
          default -> null;
        };
    if (("isempty".equals(name) || "isblank".equals(name)) && args.size() == 1) {
      SqlNode a = args.get(0);
      SqlNode toCheck = a;
      if ("isblank".equals(name)) {
        toCheck =
            new SqlBasicCall(
                SqlStdOperatorTable.REPLACE,
                List.of(
                    a, SqlLiteral.createCharString(" ", POS), SqlLiteral.createCharString("", POS)),
                POS);
        SqlNode lenZero =
            new SqlBasicCall(
                SqlStdOperatorTable.EQUALS,
                List.of(
                    new SqlBasicCall(SqlLibraryOperators.LENGTH, List.of(toCheck), POS),
                    SqlLiteral.createExactNumeric("0", POS)),
                POS);
        return new SqlBasicCall(
            SqlStdOperatorTable.OR,
            List.of(new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(a), POS), lenZero),
            POS);
      }
      return new SqlBasicCall(
          SqlStdOperatorTable.OR,
          List.of(
              new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(a), POS),
              new SqlBasicCall(PermissiveIsEmpty.INSTANCE, List.of(toCheck), POS)),
          POS);
    }
    if ("like".equals(name) && (args.size() == 2 || args.size() == 3)) {
      SqlNode escape = SqlLiteral.createCharString("\\", POS);
      SqlOperator likeOp = SqlStdOperatorTable.LIKE;
      if (args.size() == 3
          && args.get(2) instanceof SqlLiteral lit
          && Boolean.FALSE.equals(lit.getValue())) {
        likeOp = SqlLibraryOperators.ILIKE;
      } else if (args.size() == 2
          && Boolean.TRUE.equals(
              org.opensearch.sql.calcite.CalcitePlanContext.isLegacyPreferred())) {
        likeOp = SqlLibraryOperators.ILIKE;
      }
      return new SqlBasicCall(likeOp, List.of(args.get(0), args.get(1), escape), POS);
    }
    if ("atan".equals(name)) {
      return new SqlBasicCall(
          args.size() == 2 ? SqlStdOperatorTable.ATAN2 : SqlStdOperatorTable.ATAN, args, POS);
    }
    if ("log".equals(name)) {
      if (args.size() == 1) {
        return new SqlBasicCall(SqlStdOperatorTable.LN, args, POS);
      }
      if (args.size() == 2) {
        return new SqlBasicCall(SqlLibraryOperators.LOG, List.of(args.get(1), args.get(0)), POS);
      }
    }
    if ("replace".equals(name) && args.size() == 3) {
      if (args.get(1) instanceof SqlLiteral patLit && patLit.getTypeName() == SqlTypeName.CHAR) {
        String pattern = ((org.apache.calcite.util.NlsString) patLit.getValue()).getValue();
        try {
          java.util.regex.Pattern.compile(pattern);
        } catch (java.util.regex.PatternSyntaxException pse) {
          throw new IllegalArgumentException(
              String.format("Invalid regex pattern '%s': %s", pattern, pse.getDescription()), pse);
        }
      }
      SqlNode replacement = args.get(2);
      if (replacement instanceof SqlLiteral lit && lit.getTypeName() == SqlTypeName.CHAR) {
        String s = ((org.apache.calcite.util.NlsString) lit.getValue()).getValue();
        StringBuilder converted = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
          char c = s.charAt(i);
          if (c == '\\' && i + 1 < s.length() && Character.isDigit(s.charAt(i + 1))) {
            converted.append('$');
          } else if (c == '$') {
            converted.append("\\$");
          } else {
            converted.append(c);
          }
        }
        replacement = SqlLiteral.createCharString(converted.toString(), POS);
      }
      return new SqlBasicCall(
          SqlLibraryOperators.REGEXP_REPLACE_3,
          List.of(args.get(0), args.get(1), replacement),
          POS);
    }
    if ("trim".equals(name) && args.size() == 1) {
      return new SqlBasicCall(
          SqlLibraryOperators.REGEXP_REPLACE_3,
          List.of(
              args.get(0),
              SqlLiteral.createCharString("^\\s+|\\s+$", POS),
              SqlLiteral.createCharString("", POS)),
          POS);
    }
    if ("locate".equals(name) && (args.size() == 2 || args.size() == 3)) {
      List<SqlNode> swapped = new ArrayList<>();
      swapped.add(args.get(1));
      swapped.add(args.get(0));
      if (args.size() == 3) swapped.add(args.get(2));
      return new SqlBasicCall(SqlLibraryOperators.INSTR, swapped, POS);
    }
    if (args.isEmpty()) {
      switch (name) {
        case "now":
        case "current_timestamp":
        case "localtime":
        case "localtimestamp":
          return new SqlBasicCall(PPLBuiltinOperators.NOW, args, POS);
        case "curtime":
        case "current_time":
          return new SqlBasicCall(PPLBuiltinOperators.CURRENT_TIME, args, POS);
        case "curdate":
        case "current_date":
          return new SqlBasicCall(PPLBuiltinOperators.CURRENT_DATE, args, POS);
        case "utc_timestamp":
          return new SqlBasicCall(PPLBuiltinOperators.UTC_TIMESTAMP, args, POS);
        case "utc_time":
          return new SqlBasicCall(PPLBuiltinOperators.UTC_TIME, args, POS);
        default:
      }
    }
    if (("regexp_match".equals(name) || "regexp".equals(name)) && args.size() == 2) {
      SqlNode pattern = args.get(1);
      if (pattern instanceof SqlLiteral lit && lit.getTypeName() == SqlTypeName.CHAR) {
        SqlDataTypeSpec varcharSpec =
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, POS), POS);
        pattern =
            new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, List.of(pattern, varcharSpec), POS);
      }
      return new SqlBasicCall(
          SqlLibraryOperators.REGEXP_CONTAINS, List.of(args.get(0), pattern), POS);
    }
    if ("strcmp".equals(name) && args.size() == 2) {
      SqlNode a = args.get(0);
      SqlNode b = args.get(1);
      SqlNodeList whens = new SqlNodeList(POS);
      whens.add(new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(a, b), POS));
      whens.add(new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(a, b), POS));
      SqlNodeList thens = new SqlNodeList(POS);
      thens.add(SqlLiteral.createExactNumeric("-1", POS));
      thens.add(SqlLiteral.createExactNumeric("0", POS));
      return new SqlCase(POS, null, whens, thens, SqlLiteral.createExactNumeric("1", POS));
    }
    if ("split".equals(name) && args.size() == 2) {
      SqlNode str = args.get(0);
      SqlNode delim = args.get(1);
      SqlNode emptyLit = SqlLiteral.createCharString("", POS);
      SqlNode isEmpty = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(delim, emptyLit), POS);
      SqlNode anyChar = SqlLiteral.createCharString(".", POS);
      SqlNode regexExtract =
          new SqlBasicCall(SqlLibraryOperators.REGEXP_EXTRACT_ALL, List.of(str, anyChar), POS);
      SqlNode split = new SqlBasicCall(SqlLibraryOperators.SPLIT, List.of(str, delim), POS);
      SqlNodeList whens = new SqlNodeList(POS);
      whens.add(isEmpty);
      SqlNodeList thens = new SqlNodeList(POS);
      thens.add(regexExtract);
      return new SqlCase(POS, null, whens, thens, split);
    }
    if ("mvindex".equals(name) && (args.size() == 2 || args.size() == 3)) {
      SqlNode arr = args.get(0);
      SqlNode arrLen = new SqlBasicCall(SqlLibraryOperators.ARRAY_LENGTH, List.of(arr), POS);
      SqlDataTypeSpec intSpec =
          new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.INTEGER, POS), POS);
      SqlNode startIdx =
          new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, List.of(args.get(1), intSpec), POS);
      SqlNode zero = SqlLiteral.createExactNumeric("0", POS);
      SqlNode one = SqlLiteral.createExactNumeric("1", POS);
      if (args.size() == 2) {
        SqlNode isNeg =
            new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(startIdx, zero), POS);
        SqlNode lenPlusIdx =
            new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(arrLen, startIdx), POS);
        SqlNode negCase = new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(lenPlusIdx, one), POS);
        SqlNode posCase = new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(startIdx, one), POS);
        SqlNodeList caseWhens = new SqlNodeList(POS);
        caseWhens.add(isNeg);
        SqlNodeList caseThens = new SqlNodeList(POS);
        caseThens.add(negCase);
        SqlNode norm = new SqlCase(POS, null, caseWhens, caseThens, posCase);
        return new SqlBasicCall(SqlStdOperatorTable.ITEM, List.of(arr, norm), POS);
      }
      SqlNode endIdx =
          new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, List.of(args.get(2), intSpec), POS);
      SqlNode startNeg =
          new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(startIdx, zero), POS);
      SqlNode startNegCase =
          new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(arrLen, startIdx), POS);
      SqlNodeList sw = new SqlNodeList(POS);
      sw.add(startNeg);
      SqlNodeList st = new SqlNodeList(POS);
      st.add(startNegCase);
      SqlNode normStart = new SqlCase(POS, null, sw, st, startIdx);
      SqlNode endNeg = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(endIdx, zero), POS);
      SqlNode endNegCase = new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(arrLen, endIdx), POS);
      SqlNodeList ew = new SqlNodeList(POS);
      ew.add(endNeg);
      SqlNodeList et = new SqlNodeList(POS);
      et.add(endNegCase);
      SqlNode normEnd = new SqlCase(POS, null, ew, et, endIdx);
      SqlNode diff = new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(normEnd, normStart), POS);
      SqlNode len = new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(diff, one), POS);
      return new SqlBasicCall(SqlLibraryOperators.ARRAY_SLICE, List.of(arr, normStart, len), POS);
    }
    if (opOverride != null) {
      if (args.size() == 2
          && (opOverride == SqlStdOperatorTable.LIKE
              || opOverride == SqlStdOperatorTable.NOT_LIKE
              || opOverride == SqlLibraryOperators.ILIKE
              || opOverride == SqlLibraryOperators.NOT_ILIKE)) {
        List<SqlNode> withEscape = new ArrayList<>(3);
        withEscape.add(args.get(0));
        withEscape.add(args.get(1));
        withEscape.add(SqlLiteral.createCharString("\\", POS));
        return new SqlBasicCall(opOverride, withEscape, POS);
      }
      return new SqlBasicCall(opOverride, args, POS);
    }
    String resolvedName = mapPplFunctionName(name, fn.getFuncName());
    return new SqlBasicCall(
        new SqlUnresolvedFunction(
            new SqlIdentifier(resolvedName, POS),
            null,
            null,
            null,
            null,
            SqlFunctionCategory.USER_DEFINED_FUNCTION),
        args,
        POS);
  }

  SqlNode caseExpr(org.opensearch.sql.ast.expression.Case node) {
    SqlNodeList whens = new SqlNodeList(POS);
    SqlNodeList thens = new SqlNodeList(POS);
    boolean allStringResults = true;
    for (org.opensearch.sql.ast.expression.When when : node.getWhenClauses()) {
      whens.add(outer.expr(when.getCondition()));
      thens.add(outer.expr(when.getResult()));
      allStringResults &=
          when.getResult() instanceof Literal lit && lit.getType() == DataType.STRING;
    }
    SqlNode elseExpr =
        node.getElseClause().isPresent()
            ? outer.expr(node.getElseClause().get())
            : SqlLiteral.createNull(POS);
    if (node.getElseClause().isPresent()) {
      allStringResults &=
          node.getElseClause().get() instanceof Literal lit && lit.getType() == DataType.STRING;
    }
    if (allStringResults && !thens.isEmpty()) {
      SqlDataTypeSpec varcharSpec =
          new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, POS), POS);
      for (int i = 0; i < thens.size(); i++) {
        thens.set(
            i, new SqlBasicCall(SqlStdOperatorTable.CAST, List.of(thens.get(i), varcharSpec), POS));
      }
      if (node.getElseClause().isPresent()) {
        elseExpr = new SqlBasicCall(SqlStdOperatorTable.CAST, List.of(elseExpr, varcharSpec), POS);
      }
    }
    return new SqlCase(POS, null, whens, thens, elseExpr);
  }

  SqlNode inExpr(org.opensearch.sql.ast.expression.In in) {
    boolean hasString = false;
    boolean hasNumeric = false;
    for (UnresolvedExpression v : in.getValueList()) {
      if (v instanceof Literal lit) {
        DataType t = lit.getType();
        if (t == DataType.STRING) hasString = true;
        else if (t == DataType.INTEGER
            || t == DataType.LONG
            || t == DataType.SHORT
            || t == DataType.DOUBLE
            || t == DataType.FLOAT
            || t == DataType.DECIMAL) hasNumeric = true;
      }
    }
    if (hasString && hasNumeric) {
      List<String> typeNames = new ArrayList<>();
      for (UnresolvedExpression v : in.getValueList()) {
        if (v instanceof Literal lit) {
          typeNames.add(lit.getType().name());
        } else {
          typeNames.add("UNKNOWN");
        }
      }
      throw new IllegalArgumentException(
          "In expression types are incompatible: fields type LONG, values type " + typeNames);
    }
    SqlNode field = outer.expr(in.getField());
    SqlNodeList values = new SqlNodeList(POS);
    for (UnresolvedExpression v : in.getValueList()) {
      values.add(outer.expr(v));
    }
    return new SqlBasicCall(SqlStdOperatorTable.IN, List.of(field, values), POS);
  }

  SqlNode intervalExpr(org.opensearch.sql.ast.expression.Interval i) {
    Object v = i.getValue() instanceof Literal lit ? lit.getValue() : null;
    if (v == null) {
      SqlNode value = outer.expr(i.getValue());
      return new SqlBasicCall(
          new SqlUnresolvedFunction(
              new SqlIdentifier("interval", POS),
              null,
              null,
              null,
              null,
              SqlFunctionCategory.USER_DEFINED_FUNCTION),
          List.of(value, SqlLiteral.createCharString(i.getUnit().name(), POS)),
          POS);
    }
    String literalStr = v.toString();
    org.apache.calcite.avatica.util.TimeUnit unit =
        switch (i.getUnit()) {
          case MICROSECOND -> org.apache.calcite.avatica.util.TimeUnit.MICROSECOND;
          case MILLISECOND -> org.apache.calcite.avatica.util.TimeUnit.MILLISECOND;
          case SECOND -> org.apache.calcite.avatica.util.TimeUnit.SECOND;
          case MINUTE -> org.apache.calcite.avatica.util.TimeUnit.MINUTE;
          case HOUR -> org.apache.calcite.avatica.util.TimeUnit.HOUR;
          case DAY -> org.apache.calcite.avatica.util.TimeUnit.DAY;
          case WEEK -> org.apache.calcite.avatica.util.TimeUnit.WEEK;
          case MONTH -> org.apache.calcite.avatica.util.TimeUnit.MONTH;
          case QUARTER -> org.apache.calcite.avatica.util.TimeUnit.QUARTER;
          case YEAR -> org.apache.calcite.avatica.util.TimeUnit.YEAR;
          default ->
              throw new UnsupportedOperationException("Unsupported interval unit: " + i.getUnit());
        };
    SqlIntervalQualifier qualifier = new SqlIntervalQualifier(unit, null, POS);
    int sign = 1;
    if (literalStr.startsWith("-")) {
      sign = -1;
      literalStr = literalStr.substring(1);
    }
    return SqlLiteral.createInterval(sign, literalStr, qualifier, POS);
  }

  SqlNode spanExpr(Span sp) {
    SqlNode field = outer.expr(sp.getField());
    SqlNode value = outer.expr(sp.getValue());
    SpanUnit unit = sp.getUnit();
    SqlNode unitNode;
    if (unit == SpanUnit.NONE || unit == SpanUnit.UNKNOWN) {
      SqlDataTypeSpec nullSpec =
          new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.NULL, POS), POS);
      unitNode =
          new SqlBasicCall(
              SqlLibraryOperators.SAFE_CAST, List.of(SqlLiteral.createNull(POS), nullSpec), POS);
    } else {
      unitNode = SqlLiteral.createCharString(unit.getName(), POS);
    }
    return new SqlBasicCall(PPLBuiltinOperators.SPAN, List.of(field, value, unitNode), POS);
  }

  SqlNode relevanceFieldListExpr(RelevanceFieldList rfl) {
    SqlDataTypeSpec varcharSpec =
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, POS), POS);
    List<SqlNode> args = new ArrayList<>();
    for (java.util.Map.Entry<String, Float> entry : rfl.getFieldList().entrySet()) {
      args.add(
          new SqlBasicCall(
              SqlLibraryOperators.SAFE_CAST,
              List.of(SqlLiteral.createCharString(entry.getKey(), POS), varcharSpec),
              POS));
      args.add(SqlLiteral.createApproxNumeric(entry.getValue() + "E0", POS));
    }
    return new SqlBasicCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, args, POS);
  }

  SqlNode unresolvedArgExpr(UnresolvedArgument ua) {
    SqlNode value = outer.expr(ua.getValue());
    if (value instanceof SqlLiteral lit && lit.getTypeName() == SqlTypeName.CHAR) {
      SqlDataTypeSpec varcharSpec =
          new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, POS), POS);
      value = new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, List.of(value, varcharSpec), POS);
    }
    return new SqlBasicCall(
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
        List.of(SqlLiteral.createCharString(ua.getArgName(), POS), value),
        POS);
  }

  static String mapPplFunctionName(String lower, String original) {
    return switch (lower) {
      case "mvjoin" -> "ARRAY_JOIN";
      case "mvcount" -> "ARRAY_LENGTH";
      case "mvsort" -> "SORT_ARRAY";
      case "mvdedup" -> "ARRAY_DISTINCT";
      case "mvcontains" -> "ARRAY_CONTAINS";
      case "mvslice" -> "ARRAY_SLICE";
      case "mvmap" -> "transform";
      case "ifnull" -> "COALESCE";
      case "json_valid" -> "IS_JSON_VALUE";
      default -> original;
    };
  }

  static SqlOperator arithmeticOperator(String name) {
    if (name == null) return null;
    return switch (name) {
      case "+" -> SqlStdOperatorTable.PLUS;
      case "-" -> SqlStdOperatorTable.MINUS;
      case "*" -> SqlStdOperatorTable.MULTIPLY;
      case "/" -> PPLBuiltinOperators.DIVIDE;
      case "%" -> PPLBuiltinOperators.MOD;
      default -> null;
    };
  }

  static boolean isNullLiteralRef(UnresolvedExpression e) {
    QualifiedName qn = null;
    if (e instanceof QualifiedName q) qn = q;
    else if (e instanceof Field f && f.getField() instanceof QualifiedName q) qn = q;
    return qn != null && "null".equalsIgnoreCase(qn.toString());
  }

  boolean isUnknownFieldRef(UnresolvedExpression e) {
    if (outer.exprFrame == null || outer.exprFrame.currentFields == null) return false;
    QualifiedName qn = null;
    if (e instanceof QualifiedName q) qn = q;
    else if (e instanceof Field f && f.getField() instanceof QualifiedName q) qn = q;
    if (qn == null) return false;
    if (qn.getParts().size() != 1) return false;
    return !outer.exprFrame.currentFields.contains(qn.toString());
  }

  static void collectCapturedRefs(
      UnresolvedExpression e, Set<String> declared, LinkedHashMap<String, QualifiedName> out) {
    if (e == null) return;
    if (e instanceof QualifiedName qn) {
      String head = qn.getParts().isEmpty() ? qn.toString() : qn.getParts().get(0);
      if (!declared.contains(head) && !out.containsKey(head)) {
        out.put(head, qn);
      }
      return;
    }
    if (e instanceof Field fld && fld.getField() instanceof QualifiedName qn) {
      String head = qn.getParts().isEmpty() ? qn.toString() : qn.getParts().get(0);
      if (!declared.contains(head) && !out.containsKey(head)) {
        out.put(head, qn);
      }
      return;
    }
    if (e instanceof LambdaFunction nestedLf) {
      Set<String> nested = new LinkedHashSet<>(declared);
      for (QualifiedName p : nestedLf.getFuncArgs()) nested.add(p.toString());
      collectCapturedRefs(nestedLf.getFunction(), nested, out);
      return;
    }
    for (Node child : e.getChild()) {
      if (child instanceof UnresolvedExpression cu) {
        collectCapturedRefs(cu, declared, out);
      }
    }
  }

  static boolean hasStringOperand(List<UnresolvedExpression> args) {
    for (UnresolvedExpression a : args) {
      if (isStringExpr(a)) return true;
    }
    return false;
  }

  static boolean isStringExpr(UnresolvedExpression e) {
    if (e instanceof Literal lit) {
      return lit.getType() == DataType.STRING;
    }
    if (e instanceof Cast c) {
      return c.getDataType() == DataType.STRING;
    }
    if (e instanceof org.opensearch.sql.ast.expression.Function fn
        && "+".equals(fn.getFuncName())) {
      return hasStringOperand(fn.getFuncArgs());
    }
    return false;
  }

  static String pplLegacyTypeName(UnresolvedExpression e, Frame frame) {
    if (e instanceof org.opensearch.sql.ast.expression.Interval) {
      return "INTERVAL";
    }
    if (frame != null) {
      QualifiedName qn = null;
      if (e instanceof QualifiedName q) qn = q;
      else if (e instanceof Field f && f.getField() instanceof QualifiedName q) qn = q;
      if (qn != null && qn.getParts().size() == 1) {
        String name = qn.toString();
        String legacy = frame.columnLegacyTypeName.get(name);
        if (legacy != null) return legacy;
        String udt = frame.columnUdt.get(name);
        if (udt != null) return udt.toUpperCase(Locale.ROOT);
      }
    }
    DataType dt = ExpressionConverter.staticTypeOf(e, frame);
    if (dt == null) return null;
    return switch (dt) {
      case SHORT -> "SMALLINT";
      case INTEGER -> "INT";
      case LONG -> "BIGINT";
      case FLOAT -> "FLOAT";
      case DOUBLE -> "DOUBLE";
      case DECIMAL -> "DOUBLE";
      case BOOLEAN -> "BOOLEAN";
      case STRING -> "STRING";
      case DATE -> "DATE";
      case TIME -> "TIME";
      case TIMESTAMP -> "TIMESTAMP";
      case INTERVAL -> "INTERVAL";
      case IP -> "IP";
      default -> null;
    };
  }

  // ---------- castExpr ----------

  SqlNode castExpr(Cast c) {
    SqlNode value = outer.expr(c.getExpression());
    DataType targetType = c.getDataType();
    if (targetType == DataType.IP) {
      UnresolvedExpression src = c.getExpression();
      if (src instanceof Literal lit) {
        switch (lit.getType()) {
          case SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL ->
              throw new IllegalArgumentException(
                  String.format(
                      "Cannot convert %s to IP, only STRING and IP types are supported",
                      lit.getType()));
          default -> {}
        }
      }
    }
    String udtFunc =
        switch (targetType) {
          case IP -> "IP";
          case DATE -> "DATE";
          case TIME -> "TIME";
          case TIMESTAMP -> "TIMESTAMP";
          default -> null;
        };
    if (udtFunc != null) {
      return new SqlBasicCall(
          new SqlUnresolvedFunction(
              new SqlIdentifier(udtFunc, POS),
              null,
              null,
              null,
              null,
              SqlFunctionCategory.USER_DEFINED_FUNCTION),
          List.of(value),
          POS);
    }
    SqlTypeName tn = pplTypeToSqlType(targetType);
    if (tn == SqlTypeName.VARCHAR) {
      DataType srcType = ExpressionConverter.staticTypeOf(c.getExpression(), outer.exprFrame);
      if (srcType == DataType.FLOAT || srcType == DataType.DOUBLE || srcType == DataType.DECIMAL) {
        return new SqlBasicCall(
            new SqlUnresolvedFunction(
                new SqlIdentifier("NUMBER_TO_STRING", POS),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION),
            List.of(value),
            POS);
      }
      String fieldUdt = outer.qualifiedNameUdt(c.getExpression());
      if ("ip".equals(fieldUdt)) {
        return new SqlBasicCall(
            new SqlUnresolvedFunction(
                new SqlIdentifier("IP_TO_STRING", POS),
                null,
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION),
            List.of(value),
            POS);
      }
    }
    DataType srcStaticType = ExpressionConverter.staticTypeOf(c.getExpression(), outer.exprFrame);
    if (srcStaticType == DataType.BOOLEAN) {
      if (tn == SqlTypeName.INTEGER
          || tn == SqlTypeName.BIGINT
          || tn == SqlTypeName.SMALLINT
          || tn == SqlTypeName.TINYINT) {
        SqlNodeList whens = new SqlNodeList(POS);
        whens.add(value);
        SqlNodeList thens = new SqlNodeList(POS);
        thens.add(SqlLiteral.createExactNumeric("1", POS));
        return new SqlCase(POS, null, whens, thens, SqlLiteral.createExactNumeric("0", POS));
      }
      if (tn == SqlTypeName.VARCHAR) {
        SqlDataTypeSpec varcharSpec =
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, POS), POS);
        SqlNode cast =
            new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, List.of(value, varcharSpec), POS);
        return new SqlBasicCall(SqlStdOperatorTable.UPPER, List.of(cast), POS);
      }
    }
    if (tn == SqlTypeName.BOOLEAN && c.getExpression() instanceof Literal lit) {
      switch (lit.getType()) {
        case SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL:
          return new SqlBasicCall(
              SqlStdOperatorTable.NOT_EQUALS,
              List.of(value, SqlLiteral.createExactNumeric("0", POS)),
              POS);
        case STRING:
          {
            SqlNodeList whens = new SqlNodeList(POS);
            whens.add(
                new SqlBasicCall(
                    SqlStdOperatorTable.EQUALS,
                    List.of(value, SqlLiteral.createCharString("1", POS)),
                    POS));
            whens.add(
                new SqlBasicCall(
                    SqlStdOperatorTable.EQUALS,
                    List.of(value, SqlLiteral.createCharString("0", POS)),
                    POS));
            SqlNodeList thens = new SqlNodeList(POS);
            thens.add(SqlLiteral.createBoolean(true, POS));
            thens.add(SqlLiteral.createBoolean(false, POS));
            return new SqlCase(POS, null, whens, thens, SqlLiteral.createNull(POS));
          }
        default:
      }
    }
    SqlDataTypeSpec spec = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(tn, POS), POS);
    return new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, List.of(value, spec), POS);
  }

  static SqlTypeName pplTypeToSqlType(DataType t) {
    return switch (t) {
      case BOOLEAN -> SqlTypeName.BOOLEAN;
      case SHORT -> SqlTypeName.SMALLINT;
      case INTEGER -> SqlTypeName.INTEGER;
      case LONG -> SqlTypeName.BIGINT;
      case FLOAT -> SqlTypeName.FLOAT;
      case DOUBLE -> SqlTypeName.DOUBLE;
      case DECIMAL -> SqlTypeName.DECIMAL;
      case STRING -> SqlTypeName.VARCHAR;
      default ->
          throw new UnsupportedOperationException(
              "Cast target type not yet supported in PPLToSqlNodeVisitor: " + t);
    };
  }

  // ---------- Subquery exprs ----------

  SqlNode existsSubqueryExpr(org.opensearch.sql.ast.expression.subquery.ExistsSubquery es) {
    Frame subFrame = new Frame();
    Frame savedExpr = outer.exprFrame;
    outer.exprFrame = subFrame;
    SqlNode subQuery;
    try {
      subQuery =
          PPLToSqlNodeVisitor.stripImplicitMetaProjects(es.getQuery()).accept(outer, subFrame);
    } finally {
      outer.exprFrame = savedExpr;
    }
    if (subQuery instanceof SqlIdentifier
        || (subQuery instanceof SqlBasicCall sbc && sbc.getOperator() == SqlStdOperatorTable.AS)) {
      SqlNodeList star = new SqlNodeList(POS);
      star.add(SqlIdentifier.star(POS));
      subQuery = SqlBuilder.select(star).from(subQuery).wrap(subFrame);
    }
    return new SqlBasicCall(SqlStdOperatorTable.EXISTS, List.of(subQuery), POS);
  }

  SqlNode scalarSubqueryExpr(org.opensearch.sql.ast.expression.subquery.ScalarSubquery ss) {
    Frame subFrame = new Frame();
    Frame savedExpr = outer.exprFrame;
    outer.exprFrame = subFrame;
    try {
      return PPLToSqlNodeVisitor.stripImplicitMetaProjects(ss.getQuery()).accept(outer, subFrame);
    } finally {
      outer.exprFrame = savedExpr;
    }
  }

  SqlNode inSubqueryExpr(org.opensearch.sql.ast.expression.subquery.InSubquery is) {
    Frame subFrame = new Frame();
    Frame savedExpr = outer.exprFrame;
    outer.exprFrame = subFrame;
    SqlNode subQuery;
    try {
      subQuery =
          PPLToSqlNodeVisitor.stripImplicitMetaProjects(is.getQuery()).accept(outer, subFrame);
    } finally {
      outer.exprFrame = savedExpr;
    }
    int lhsCount = is.getValue().size();
    int rhsCount = subFrame.currentFields == null ? -1 : subFrame.currentFields.size();
    if (rhsCount > 0 && lhsCount != rhsCount) {
      throw new IllegalArgumentException(
          "The number of columns in the left hand side of an IN subquery does not match the "
              + "number of columns in the output of subquery");
    }
    SqlNode left;
    if (is.getValue().size() == 1) {
      left = outer.expr(is.getValue().get(0));
    } else {
      List<SqlNode> rowOperands = new ArrayList<>(is.getValue().size());
      for (UnresolvedExpression v : is.getValue()) {
        rowOperands.add(outer.expr(v));
      }
      left = new SqlBasicCall(SqlStdOperatorTable.ROW, rowOperands, POS);
    }
    return new SqlBasicCall(SqlStdOperatorTable.IN, List.of(left, subQuery), POS);
  }

  // ---------- Identifier helpers ----------

  SqlIdentifier toIdentifier(String name) {
    List<String> parts =
        name.indexOf('.') < 0 ? List.of(name) : java.util.Arrays.asList(name.split("\\."));
    Frame f = outer.exprFrame;
    if (f != null
        && !f.aliasSynonyms.isEmpty()
        && parts.size() >= 2
        && f.aliasSynonyms.containsKey(parts.get(0))) {
      List<String> rewritten = new ArrayList<>(parts);
      rewritten.set(0, f.aliasSynonyms.get(parts.get(0)));
      return ExpressionConverter.quotedIdentifier(rewritten);
    }
    if (parts.size() >= 2
        && f != null
        && !f.liveJoinAliases.contains(parts.get(0))
        && f.aliasSynonyms.isEmpty()
        && f.currentFields != null
        && f.currentFields.contains(name)) {
      return ExpressionConverter.quotedIdentifier(java.util.Collections.singletonList(name));
    }
    if (parts.size() >= 2 && f != null && f.dottedEvalAliases.contains(name)) {
      return ExpressionConverter.quotedIdentifier(java.util.Collections.singletonList(name));
    }
    return ExpressionConverter.quotedIdentifier(parts);
  }

  SqlIdentifier qualifyIfAmbiguous(String name, Frame frame) {
    JoinHints hints = frame.joinHints;
    if (name.indexOf('.') < 0
        && hints != null
        && hints.leftAlias() != null
        && hints.ambiguousColumns().contains(name)) {
      return new SqlIdentifier(java.util.Arrays.asList(hints.leftAlias(), name), POS);
    }
    return toIdentifier(name);
  }

  // ---------- Top-level expression dispatch ----------

  SqlNode expr(UnresolvedExpression e) {
    if (e instanceof Literal lit) {
      return ExpressionConverter.literalToSqlNode(lit);
    }
    if (e instanceof QualifiedName qn) {
      List<String> parts = qn.getParts();
      Frame f = outer.exprFrame;
      if (f != null
          && !f.aliasSynonyms.isEmpty()
          && parts.size() >= 2
          && f.aliasSynonyms.containsKey(parts.get(0))) {
        List<String> rewritten = new ArrayList<>(parts);
        rewritten.set(0, f.aliasSynonyms.get(parts.get(0)));
        return new SqlIdentifier(rewritten, POS);
      }
      if (parts.size() >= 2 && f != null && f.dottedEvalAliases.contains(qn.toString())) {
        return ExpressionConverter.quotedIdentifier(
            java.util.Collections.singletonList(qn.toString()));
      }
      if (parts.size() >= 2
          && f != null
          && !f.liveJoinAliases.contains(parts.get(0))
          && f.aliasSynonyms.isEmpty()
          && f.currentFields != null
          && f.currentFields.contains(qn.toString())) {
        return ExpressionConverter.quotedIdentifier(
            java.util.Collections.singletonList(qn.toString()));
      }
      if (parts.size() >= 2 && f != null && f.mapColumns.contains(parts.get(0))) {
        String subkey = String.join(".", parts.subList(1, parts.size()));
        return new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(new SqlIdentifier(parts.get(0), POS), SqlLiteral.createCharString(subkey, POS)),
            POS);
      }
      if (parts.size() >= 3
          && f != null
          && f.liveJoinAliases.contains(parts.get(0))
          && f.mapColumns.contains(parts.get(1))) {
        String subkey = String.join(".", parts.subList(2, parts.size()));
        SqlIdentifier mapRef = new SqlIdentifier(parts.subList(0, 2), POS);
        return new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(mapRef, SqlLiteral.createCharString(subkey, POS)),
            POS);
      }
      return new SqlIdentifier(parts, POS);
    }
    if (e instanceof Field f) {
      return expr(f.getField());
    }
    if (e instanceof org.opensearch.sql.ast.expression.Compare c) {
      String cOp = c.getOperator() == null ? "" : c.getOperator().toLowerCase(Locale.ROOT);
      if (cOp.equals("=") || cOp.equals("!=") || cOp.equals("<>")) {
        Boolean rightBool = ExpressionConverter.extractBoolLiteral(c.getRight());
        Boolean leftBool = ExpressionConverter.extractBoolLiteral(c.getLeft());
        UnresolvedExpression fieldAst = null;
        Boolean boolValue = null;
        if (rightBool != null) {
          fieldAst = c.getLeft();
          boolValue = rightBool;
        } else if (leftBool != null) {
          fieldAst = c.getRight();
          boolValue = leftBool;
        }
        if (boolValue != null
            && ExpressionConverter.staticTypeOf(fieldAst, outer.exprFrame) == DataType.BOOLEAN) {
          SqlNode fieldSide = expr(fieldAst);
          boolean isEq = cOp.equals("=");
          if (isEq) {
            return boolValue
                ? fieldSide
                : new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(fieldSide), POS);
          } else {
            SqlOperator postfix =
                boolValue ? SqlStdOperatorTable.IS_NOT_TRUE : SqlStdOperatorTable.IS_NOT_FALSE;
            return new SqlBasicCall(postfix, List.of(fieldSide), POS);
          }
        }
      }
      SqlNode l = expr(c.getLeft());
      SqlNode r = expr(c.getRight());
      String lDtName = ExpressionConverter.staticDateTimeUdtName(c.getLeft());
      String rDtName = ExpressionConverter.staticDateTimeUdtName(c.getRight());
      if (lDtName != null && rDtName != null && !lDtName.equals(rDtName)) {
        if (!"timestamp".equals(lDtName)) {
          l = new SqlBasicCall(PPLBuiltinOperators.TIMESTAMP, List.of(l), POS);
        }
        if (!"timestamp".equals(rDtName)) {
          r = new SqlBasicCall(PPLBuiltinOperators.TIMESTAMP, List.of(r), POS);
        }
      }
      String lFieldUdt = outer.qualifiedNameUdt(c.getLeft());
      String rFieldUdt = outer.qualifiedNameUdt(c.getRight());
      SqlDataTypeSpec varcharSpec =
          new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, POS), POS);
      if (lFieldUdt != null && ExpressionConverter.isStringLiteral(c.getRight())) {
        SqlOperator op = ExpressionConverter.udtConstructorOpForRoot(lFieldUdt);
        if (op != null) {
          SqlNode rArg =
              new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, List.of(r, varcharSpec), POS);
          r = new SqlBasicCall(op, List.of(rArg), POS);
        }
      } else if (rFieldUdt != null && ExpressionConverter.isStringLiteral(c.getLeft())) {
        SqlOperator op = ExpressionConverter.udtConstructorOpForRoot(rFieldUdt);
        if (op != null) {
          SqlNode lArg =
              new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, List.of(l, varcharSpec), POS);
          l = new SqlBasicCall(op, List.of(lArg), POS);
        }
      }
      if ("ip".equals(lFieldUdt) || "ip".equals(rFieldUdt)) {
        SqlOperator ipOp =
            ExpressionConverter.ipComparisonOperator(
                c.getOperator() == null ? "" : c.getOperator().toLowerCase(Locale.ROOT));
        if (ipOp != null) {
          return new SqlBasicCall(ipOp, List.of(l, r), POS);
        }
      }
      return new SqlBasicCall(
          ExpressionConverter.comparisonOperator(c.getOperator()), List.of(l, r), POS);
    }
    if (e instanceof org.opensearch.sql.ast.expression.And a) {
      return new SqlBasicCall(
          SqlStdOperatorTable.AND, List.of(expr(a.getLeft()), expr(a.getRight())), POS);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Or o) {
      return new SqlBasicCall(
          SqlStdOperatorTable.OR, List.of(expr(o.getLeft()), expr(o.getRight())), POS);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Not n) {
      UnresolvedExpression inner = n.getExpression();
      if (inner instanceof org.opensearch.sql.ast.expression.Compare innerCmp) {
        String iop =
            innerCmp.getOperator() == null ? "" : innerCmp.getOperator().toLowerCase(Locale.ROOT);
        if (iop.equals("=") || iop.equals("!=") || iop.equals("<>")) {
          Boolean rb = ExpressionConverter.extractBoolLiteral(innerCmp.getRight());
          Boolean lb = ExpressionConverter.extractBoolLiteral(innerCmp.getLeft());
          UnresolvedExpression fieldAst = null;
          if (rb != null) fieldAst = innerCmp.getLeft();
          else if (lb != null) fieldAst = innerCmp.getRight();
          if (fieldAst != null
              && ExpressionConverter.staticTypeOf(fieldAst, outer.exprFrame) == DataType.BOOLEAN) {
            String flipped = iop.equals("=") ? "!=" : "=";
            org.opensearch.sql.ast.expression.Compare flippedCmp =
                new org.opensearch.sql.ast.expression.Compare(
                    flipped, innerCmp.getLeft(), innerCmp.getRight());
            return expr(flippedCmp);
          }
        }
      }
      return new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(expr(n.getExpression())), POS);
    }
    if (e instanceof Cast c) {
      return castExpr(c);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Function fn) {
      return funcExpr(fn);
    }
    if (e instanceof Span sp) {
      return spanExpr(sp);
    }
    if (e instanceof org.opensearch.sql.ast.expression.In in) {
      return inExpr(in);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Case caseE) {
      return caseExpr(caseE);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Interval interval) {
      return intervalExpr(interval);
    }
    if (e instanceof UnresolvedArgument ua) {
      return unresolvedArgExpr(ua);
    }
    if (e instanceof RelevanceFieldList rfl) {
      return relevanceFieldListExpr(rfl);
    }
    if (e instanceof LambdaFunction lf) {
      SqlNodeList paramList = new SqlNodeList(POS);
      for (QualifiedName qn : lf.getFuncArgs()) {
        paramList.add(new SqlIdentifier(qn.toString(), POS));
      }
      SqlNode body = expr(lf.getFunction());
      return new org.apache.calcite.sql.SqlLambda(POS, paramList, body);
    }
    if (e instanceof org.opensearch.sql.ast.expression.subquery.InSubquery is) {
      return inSubqueryExpr(is);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Between b) {
      DataType lowerType = b.getLowerBound() instanceof Literal l ? l.getType() : null;
      DataType upperType = b.getUpperBound() instanceof Literal u ? u.getType() : null;
      if (lowerType != null
          && upperType != null
          && !PPLToSqlNodeVisitor.pplBetweenBoundsCompatible(lowerType, upperType)) {
        throw new IllegalArgumentException(
            "BETWEEN expression types are incompatible: lower bound is "
                + PPLToSqlNodeVisitor.pplTypeToSqlNameForError(lowerType)
                + ", upper bound is "
                + PPLToSqlNodeVisitor.pplTypeToSqlNameForError(upperType));
      }
      return new SqlBasicCall(
          SqlStdOperatorTable.BETWEEN,
          List.of(expr(b.getValue()), expr(b.getLowerBound()), expr(b.getUpperBound())),
          POS);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Xor xor) {
      SqlNode l = expr(xor.getLeft());
      SqlNode r = expr(xor.getRight());
      SqlNode or = new SqlBasicCall(SqlStdOperatorTable.OR, List.of(l, r), POS);
      SqlNode and = new SqlBasicCall(SqlStdOperatorTable.AND, List.of(l, r), POS);
      SqlNode notAnd = new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(and), POS);
      return new SqlBasicCall(SqlStdOperatorTable.AND, List.of(or, notAnd), POS);
    }
    if (e instanceof org.opensearch.sql.ast.expression.subquery.ExistsSubquery es) {
      return existsSubqueryExpr(es);
    }
    if (e instanceof org.opensearch.sql.ast.expression.subquery.ScalarSubquery ss) {
      return scalarSubqueryExpr(ss);
    }
    throw new UnsupportedOperationException(
        "Expression not yet supported in PPLToSqlNodeVisitor: " + e.getClass().getSimpleName());
  }

  // ---------- aggCall ----------

  SqlNode aggCall(UnresolvedExpression e) {
    return aggCall(e, false);
  }

  SqlNode aggCall(UnresolvedExpression e, boolean windowed) {
    if (!(e instanceof org.opensearch.sql.ast.expression.AggregateFunction af)) {
      throw new UnsupportedOperationException(
          "stats aggregator must be an AggregateFunction, got: " + e.getClass().getSimpleName());
    }
    String fnLower = af.getFuncName().toLowerCase(Locale.ROOT);
    boolean distinct = Boolean.TRUE.equals(af.getDistinct());
    if (fnLower.equals("dc")
        || fnLower.equals("distinct_count")
        || fnLower.equals("distinct_count_approx")) {
      fnLower = "count";
      distinct = true;
    } else if (fnLower.equals("c")) {
      fnLower = "count";
    }
    if (fnLower.equals("earliest") || fnLower.equals("latest")) {
      List<SqlNode> args = new ArrayList<>();
      UnresolvedExpression argExpr0 = af.getField();
      args.add(
          argExpr0 instanceof org.opensearch.sql.ast.expression.AllFields
              ? SqlIdentifier.star(POS)
              : expr(argExpr0));
      if (af.getArgList() != null) {
        for (UnresolvedExpression extra : af.getArgList()) {
          if (extra instanceof UnresolvedArgument ua) {
            args.add(expr(ua.getValue()));
          } else {
            args.add(expr(extra));
          }
        }
      }
      if (args.size() == 1) {
        args.add(
            new SqlIdentifier(
                org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP, POS));
      }
      SqlOperator op0 =
          fnLower.equals("earliest") ? SqlStdOperatorTable.ARG_MIN : SqlStdOperatorTable.ARG_MAX;
      return new SqlBasicCall(op0, args, POS);
    }
    org.apache.calcite.sql.SqlAggFunction op =
        switch (fnLower) {
          case "count" -> SqlStdOperatorTable.COUNT;
          case "sum" -> SqlStdOperatorTable.SUM;
          case "avg" -> windowed ? SqlStdOperatorTable.AVG : PPLBuiltinOperators.AVG_NULLABLE;
          case "min" -> SqlStdOperatorTable.MIN;
          case "max" -> SqlStdOperatorTable.MAX;
          case "stddev", "stddev_samp" ->
              windowed ? SqlStdOperatorTable.STDDEV_SAMP : PPLBuiltinOperators.STDDEV_SAMP_NULLABLE;
          case "stddev_pop" ->
              windowed ? SqlStdOperatorTable.STDDEV_POP : PPLBuiltinOperators.STDDEV_POP_NULLABLE;
          case "variance", "var_samp" ->
              windowed ? SqlStdOperatorTable.VAR_SAMP : PPLBuiltinOperators.VAR_SAMP_NULLABLE;
          case "var_pop" ->
              windowed ? SqlStdOperatorTable.VAR_POP : PPLBuiltinOperators.VAR_POP_NULLABLE;
          case "percentile", "percentile_approx", "median" -> PPLBuiltinOperators.PERCENTILE_APPROX;
          case "list" -> PPLBuiltinOperators.LIST;
          case "values" -> PPLBuiltinOperators.VALUES;
          case "first" -> PPLBuiltinOperators.FIRST;
          case "last" -> PPLBuiltinOperators.LAST;
          case "take" -> PPLBuiltinOperators.TAKE;
          default ->
              throw new UnsupportedOperationException(
                  "Aggregate function not yet supported in PPLToSqlNodeVisitor: " + fnLower);
        };
    UnresolvedExpression argExpr = af.getField();
    SqlNode arg;
    if (argExpr instanceof org.opensearch.sql.ast.expression.AllFields) {
      arg = SqlIdentifier.star(POS);
    } else {
      arg = expr(argExpr);
    }
    boolean isNumericAgg =
        fnLower.equals("sum")
            || fnLower.equals("avg")
            || fnLower.equals("min")
            || fnLower.equals("max")
            || fnLower.equals("stddev")
            || fnLower.equals("stddev_samp")
            || fnLower.equals("stddev_pop")
            || fnLower.equals("var_samp")
            || fnLower.equals("var_pop")
            || fnLower.equals("median")
            || fnLower.equals("percentile")
            || fnLower.equals("percentile_approx");
    if (isNumericAgg
        && arg instanceof SqlBasicCall sbc
        && sbc.getOperator() == SqlStdOperatorTable.ITEM) {
      SqlDataTypeSpec doubleSpec =
          new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.DOUBLE, POS), POS);
      arg = new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, List.of(arg, doubleSpec), POS);
    }
    SqlLiteral quantifier =
        distinct ? org.apache.calcite.sql.SqlSelectKeyword.DISTINCT.symbol(POS) : null;
    boolean takesExtraArgs =
        op == PPLBuiltinOperators.PERCENTILE_APPROX
            || op == PPLBuiltinOperators.FIRST
            || op == PPLBuiltinOperators.LAST
            || op == PPLBuiltinOperators.TAKE
            || op == PPLBuiltinOperators.VALUES;
    if (takesExtraArgs) {
      List<SqlNode> args = new ArrayList<>();
      args.add(arg);
      if (af.getArgList() != null) {
        for (UnresolvedExpression extra : af.getArgList()) {
          if (extra instanceof UnresolvedArgument ua) {
            args.add(expr(ua.getValue()));
          } else {
            args.add(expr(extra));
          }
        }
      }
      if (fnLower.equals("median") && args.size() == 1) {
        args.add(SqlLiteral.createExactNumeric("50", POS));
      }
      if (fnLower.equals("percentile")
          || fnLower.equals("percentile_approx")
          || fnLower.equals("median")) {
        DataType fieldDt = ExpressionConverter.staticTypeOf(argExpr, outer.exprFrame);
        if (fieldDt != null) {
          try {
            SqlTypeName sqlType = pplTypeToSqlType(fieldDt);
            args.add(SqlLiteral.createSymbol(sqlType, POS));
          } catch (UnsupportedOperationException ignored) {
          }
        }
      }
      return new SqlBasicCall(op, args, POS, quantifier);
    }
    return new SqlBasicCall(op, List.of(arg), POS, quantifier);
  }
}
