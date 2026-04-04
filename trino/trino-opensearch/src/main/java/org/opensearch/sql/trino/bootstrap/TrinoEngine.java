/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.bootstrap;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Lightweight Trino-compatible SQL execution engine embedded within the OpenSearch plugin.
 *
 * <p>For the initial PoC, this engine evaluates simple constant expressions (e.g., {@code SELECT
 * 1}, {@code SELECT 'hello'}) without requiring the full Trino runtime. Results are returned in
 * Trino client protocol JSON format.
 *
 * <p>Future iterations will integrate the full Trino query engine via an isolated classloader to
 * support complex queries, catalogs, and connectors.
 */
public class TrinoEngine implements Closeable {

  private static final Logger LOG = LogManager.getLogger(TrinoEngine.class);

  // Pattern to match SELECT with one or more expressions (no FROM clause)
  private static final Pattern SELECT_PATTERN =
      Pattern.compile(
          "^\\s*SELECT\\s+(.+?)\\s*$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public TrinoEngine() {
    LOG.info("TrinoEngine initialized (lightweight constant-expression evaluator)");
  }

  /**
   * Execute a SQL query and return the result in Trino client protocol JSON format.
   *
   * @param sql the SQL query to execute
   * @return JSON string in Trino v1/statement response format
   */
  public String executeAndSerializeJson(String sql) {
    String queryId = UUID.randomUUID().toString().replace("-", "");
    LOG.debug("TrinoEngine executing query [{}] with id [{}]", sql, queryId);

    try {
      QueryResult result = execute(sql);
      return serializeToTrinoJson(queryId, result);
    } catch (Exception e) {
      LOG.error("Query execution failed: {}", e.getMessage(), e);
      return serializeErrorJson(queryId, e.getMessage());
    }
  }

  /** Parse and execute a SQL statement, returning a structured result. */
  QueryResult execute(String sql) {
    String trimmed = sql.trim();
    // Remove trailing semicolons
    if (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }

    Matcher selectMatcher = SELECT_PATTERN.matcher(trimmed);
    if (!selectMatcher.matches()) {
      throw new UnsupportedOperationException(
          "Only SELECT queries with constant expressions are supported in this PoC. Got: " + sql);
    }

    String expressionList = selectMatcher.group(1).trim();

    // Check for FROM clause - not supported in lightweight mode
    if (expressionList.toUpperCase().contains(" FROM ")) {
      throw new UnsupportedOperationException(
          "SELECT with FROM clause is not yet supported in lightweight mode. Got: " + sql);
    }

    // Parse comma-separated expressions
    List<String> expressions = splitExpressions(expressionList);
    List<ColumnDef> columns = new ArrayList<>();
    List<Object> row = new ArrayList<>();

    for (int i = 0; i < expressions.size(); i++) {
      String expr = expressions.get(i).trim();
      EvalResult evalResult = evaluateExpression(expr, i);
      columns.add(evalResult.column);
      row.add(evalResult.value);
    }

    QueryResult result = new QueryResult();
    result.columns = columns;
    result.rows = List.of(row);
    return result;
  }

  /**
   * Split expression list by commas, respecting parentheses and string literals.
   */
  private List<String> splitExpressions(String exprList) {
    List<String> result = new ArrayList<>();
    int depth = 0;
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    StringBuilder current = new StringBuilder();

    for (int i = 0; i < exprList.length(); i++) {
      char c = exprList.charAt(i);

      if (c == '\'' && !inDoubleQuote) {
        // Check for escaped quote
        if (inSingleQuote && i + 1 < exprList.length() && exprList.charAt(i + 1) == '\'') {
          current.append("''");
          i++;
          continue;
        }
        inSingleQuote = !inSingleQuote;
      } else if (c == '"' && !inSingleQuote) {
        inDoubleQuote = !inDoubleQuote;
      } else if (!inSingleQuote && !inDoubleQuote) {
        if (c == '(') {
          depth++;
        } else if (c == ')') {
          depth--;
        } else if (c == ',' && depth == 0) {
          result.add(current.toString());
          current = new StringBuilder();
          continue;
        }
      }
      current.append(c);
    }
    result.add(current.toString());
    return result;
  }

  /** Evaluate a single expression and return the column definition and value. */
  private EvalResult evaluateExpression(String expr, int index) {
    String trimmed = expr.trim();

    // Check for alias: expression AS alias
    String alias = null;
    Pattern aliasPattern =
        Pattern.compile("^(.+?)\\s+(?:AS\\s+)?(\\w+)\\s*$", Pattern.CASE_INSENSITIVE);
    // More specific: look for AS keyword
    Pattern asPattern =
        Pattern.compile(
            "^(.+?)\\s+AS\\s+\"?([\\w]+)\"?\\s*$", Pattern.CASE_INSENSITIVE);
    Matcher asMatcher = asPattern.matcher(trimmed);
    if (asMatcher.matches()) {
      trimmed = asMatcher.group(1).trim();
      alias = asMatcher.group(2);
    }

    // Integer literal
    if (trimmed.matches("-?\\d+")) {
      long val = Long.parseLong(trimmed);
      String colName = alias != null ? alias : "_col" + index;
      if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
        return new EvalResult(new ColumnDef(colName, "integer", "integer"), (int) val);
      }
      return new EvalResult(new ColumnDef(colName, "bigint", "bigint"), val);
    }

    // Decimal/float literal
    if (trimmed.matches("-?\\d+\\.\\d+([eE][+-]?\\d+)?")) {
      double val = Double.parseDouble(trimmed);
      String colName = alias != null ? alias : "_col" + index;
      return new EvalResult(new ColumnDef(colName, "double", "double"), val);
    }

    // String literal (single quotes)
    if (trimmed.startsWith("'") && trimmed.endsWith("'") && trimmed.length() >= 2) {
      String val = trimmed.substring(1, trimmed.length() - 1).replace("''", "'");
      String colName = alias != null ? alias : "_col" + index;
      int len = val.length();
      String typeName = "varchar(" + len + ")";
      return new EvalResult(new ColumnDef(colName, typeName, "varchar"), val);
    }

    // Boolean literals
    if (trimmed.equalsIgnoreCase("true") || trimmed.equalsIgnoreCase("false")) {
      boolean val = Boolean.parseBoolean(trimmed.toLowerCase());
      String colName = alias != null ? alias : "_col" + index;
      return new EvalResult(new ColumnDef(colName, "boolean", "boolean"), val);
    }

    // NULL literal
    if (trimmed.equalsIgnoreCase("null")) {
      String colName = alias != null ? alias : "_col" + index;
      return new EvalResult(new ColumnDef(colName, "unknown", "unknown"), null);
    }

    // Simple arithmetic: two integer operands with +, -, *, /
    Pattern arithPattern = Pattern.compile("^(-?\\d+)\\s*([+\\-*/])\\s*(-?\\d+)$");
    Matcher arithMatcher = arithPattern.matcher(trimmed);
    if (arithMatcher.matches()) {
      long left = Long.parseLong(arithMatcher.group(1));
      String op = arithMatcher.group(2);
      long right = Long.parseLong(arithMatcher.group(3));
      long result =
          switch (op) {
            case "+" -> left + right;
            case "-" -> left - right;
            case "*" -> left * right;
            case "/" -> {
              if (right == 0) {
                throw new ArithmeticException("Division by zero");
              }
              yield left / right;
            }
            default -> throw new UnsupportedOperationException("Unknown operator: " + op);
          };
      String colName = alias != null ? alias : "_col" + index;
      if (result >= Integer.MIN_VALUE && result <= Integer.MAX_VALUE) {
        return new EvalResult(new ColumnDef(colName, "integer", "integer"), (int) result);
      }
      return new EvalResult(new ColumnDef(colName, "bigint", "bigint"), result);
    }

    // CAST expression: CAST(value AS type)
    Pattern castPattern =
        Pattern.compile(
            "^CAST\\s*\\(\\s*(.+?)\\s+AS\\s+(\\w+(?:\\s*\\(\\s*\\d+\\s*(?:,\\s*\\d+\\s*)?\\))?)\\s*\\)$",
            Pattern.CASE_INSENSITIVE);
    Matcher castMatcher = castPattern.matcher(trimmed);
    if (castMatcher.matches()) {
      String innerExpr = castMatcher.group(1).trim();
      String targetType = castMatcher.group(2).trim().toLowerCase();
      EvalResult inner = evaluateExpression(innerExpr, index);
      Object castedValue = castValue(inner.value, targetType);
      String colName = alias != null ? alias : "_col" + index;
      return new EvalResult(new ColumnDef(colName, targetType, targetType), castedValue);
    }

    throw new UnsupportedOperationException(
        "Cannot evaluate expression: "
            + trimmed
            + ". Only literals and simple arithmetic are supported.");
  }

  private Object castValue(Object value, String targetType) {
    if (value == null) {
      return null;
    }
    return switch (targetType) {
      case "integer", "int" -> {
        if (value instanceof Number n) {
          yield n.intValue();
        }
        yield Integer.parseInt(value.toString());
      }
      case "bigint" -> {
        if (value instanceof Number n) {
          yield n.longValue();
        }
        yield Long.parseLong(value.toString());
      }
      case "double" -> {
        if (value instanceof Number n) {
          yield n.doubleValue();
        }
        yield Double.parseDouble(value.toString());
      }
      case "varchar", "char" -> value.toString();
      case "boolean" -> {
        if (value instanceof Boolean) {
          yield value;
        }
        yield Boolean.parseBoolean(value.toString());
      }
      default -> value;
    };
  }

  /** Serialize a successful query result to Trino client protocol JSON. */
  private String serializeToTrinoJson(String queryId, QueryResult result) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"id\":\"").append(queryId).append("\",");

    // Columns
    sb.append("\"columns\":[");
    for (int i = 0; i < result.columns.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      ColumnDef col = result.columns.get(i);
      sb.append("{\"name\":\"").append(escapeJson(col.name)).append("\",");
      sb.append("\"type\":\"").append(escapeJson(col.type)).append("\",");
      sb.append("\"typeSignature\":{\"rawType\":\"")
          .append(escapeJson(col.rawType))
          .append("\"}}");
    }
    sb.append("],");

    // Data
    sb.append("\"data\":[");
    for (int r = 0; r < result.rows.size(); r++) {
      if (r > 0) {
        sb.append(",");
      }
      sb.append("[");
      List<Object> row = result.rows.get(r);
      for (int c = 0; c < row.size(); c++) {
        if (c > 0) {
          sb.append(",");
        }
        Object val = row.get(c);
        if (val == null) {
          sb.append("null");
        } else if (val instanceof String s) {
          sb.append("\"").append(escapeJson(s)).append("\"");
        } else if (val instanceof Boolean) {
          sb.append(val.toString());
        } else {
          sb.append(val.toString());
        }
      }
      sb.append("]");
    }
    sb.append("],");

    // Stats
    sb.append("\"stats\":{");
    sb.append("\"state\":\"FINISHED\",");
    sb.append("\"queued\":false,");
    sb.append("\"scheduled\":true,");
    sb.append("\"nodes\":1,");
    sb.append("\"totalSplits\":1,");
    sb.append("\"queuedSplits\":0,");
    sb.append("\"runningSplits\":0,");
    sb.append("\"completedSplits\":1,");
    sb.append("\"cpuTimeMillis\":0,");
    sb.append("\"wallTimeMillis\":0,");
    sb.append("\"queuedTimeMillis\":0,");
    sb.append("\"elapsedTimeMillis\":0,");
    sb.append("\"processedRows\":0,");
    sb.append("\"processedBytes\":0,");
    sb.append("\"peakMemoryBytes\":0");
    sb.append("}}");

    return sb.toString();
  }

  /** Serialize an error response in Trino client protocol format. */
  private String serializeErrorJson(String queryId, String message) {
    return "{"
        + "\"id\":\""
        + queryId
        + "\","
        + "\"stats\":{\"state\":\"FAILED\"},"
        + "\"error\":{"
        + "\"message\":\""
        + escapeJson(message)
        + "\","
        + "\"errorCode\":1,"
        + "\"errorName\":\"GENERIC_INTERNAL_ERROR\","
        + "\"errorType\":\"INTERNAL_ERROR\""
        + "}}";
  }

  private static String escapeJson(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }

  @Override
  public void close() {
    LOG.info("TrinoEngine closed");
  }

  /** Internal representation of a query result. */
  static class QueryResult {
    List<ColumnDef> columns;
    List<List<Object>> rows;
  }

  /** Column definition for the result schema. */
  static class ColumnDef {
    final String name;
    final String type;
    final String rawType;

    ColumnDef(String name, String type, String rawType) {
      this.name = name;
      this.type = type;
      this.rawType = rawType;
    }
  }

  /** Result of evaluating a single expression. */
  private static class EvalResult {
    final ColumnDef column;
    final Object value;

    EvalResult(ColumnDef column, Object value) {
      this.column = column;
      this.value = value;
    }
  }
}
