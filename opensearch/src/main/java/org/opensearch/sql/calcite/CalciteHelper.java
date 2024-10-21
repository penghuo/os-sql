/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CalciteHelper {

  /** Matches a number with at least four zeros after the point. */
  private static final Pattern TRAILING_ZERO_PATTERN =
      Pattern.compile("-?[0-9]+\\.([0-9]*[1-9])?(00000*[0-9][0-9]?)");

  /** Matches a number with at least four nines after the point. */
  private static final Pattern TRAILING_NINE_PATTERN =
      Pattern.compile("-?[0-9]+\\.([0-9]*[0-8])?(99999*[0-9][0-9]?)");

  /** Converts a {@link ResultSet} to string. */
  static class ResultSetFormatter {
    final StringBuilder buf = new StringBuilder();

    public ResultSetFormatter resultSet(ResultSet resultSet)
        throws SQLException {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        rowToString(resultSet, metaData);
        buf.append("\n");
      }
      return this;
    }

    /** Converts one row to a string. */
    ResultSetFormatter rowToString(ResultSet resultSet,
                                   ResultSetMetaData metaData) throws SQLException {
      int n = metaData.getColumnCount();
      if (n > 0) {
        for (int i = 1;; i++) {
          buf.append(metaData.getColumnLabel(i))
              .append("=")
              .append(adjustValue(resultSet.getString(i)));
          if (i == n) {
            break;
          }
          buf.append("; ");
        }
      }
      return this;
    }

    protected String adjustValue(String string) {
      if (string != null) {
        string = correctRoundedFloat(string);
      }
      return string;
    }

    /** Removes floating-point rounding errors from the end of a string.
     *
     * <p>{@code 12.300000006} becomes {@code 12.3};
     * {@code -12.37999999991} becomes {@code -12.38}. */
    public static String correctRoundedFloat(String s) {
      if (s == null) {
        return s;
      }
      final Matcher m = TRAILING_ZERO_PATTERN.matcher(s);
      if (m.matches()) {
        s = s.substring(0, s.length() - m.group(2).length());
      }
      final Matcher m2 = TRAILING_NINE_PATTERN.matcher(s);
      if (m2.matches()) {
        s = s.substring(0, s.length() - m2.group(2).length());
        if (s.length() > 0) {
          final char c = s.charAt(s.length() - 1);
          switch (c) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case  '8':
              // '12.3499999996' became '12.34', now we make it '12.35'
              s = s.substring(0, s.length() - 1) + (char) (c + 1);
              break;
            case '.':
              // '12.9999991' became '12.', which we leave as is.
              break;
          }
        }
      }
      return s;
    }

    public Collection<String> toStringList(ResultSet resultSet,
                                           Collection<String> list) throws SQLException {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        rowToString(resultSet, metaData);
        list.add(buf.toString());
        buf.setLength(0);
      }
      return list;
    }

    /** Flushes the buffer and returns its previous contents. */
    public String string() {
      String s = buf.toString();
      buf.setLength(0);
      return s;
    }
  }
}
