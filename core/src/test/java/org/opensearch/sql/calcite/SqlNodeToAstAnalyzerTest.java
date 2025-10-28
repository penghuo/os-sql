/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;

class SqlNodeToAstAnalyzerTest {

  private static final SqlParserPos POS = SqlParserPos.ZERO;
  private final SqlNodeToAstAnalyzer analyzer = new SqlNodeToAstAnalyzer();

  @Test
  void convertLowerFunction() {
    SqlIdentifier identifier = new SqlIdentifier(List.of("name"), POS);
    SqlNode[] operands = new SqlNode[] {identifier};
    SqlBasicCall call = new SqlBasicCall(SqlStdOperatorTable.LOWER, operands, POS);

    Function function = analyzer.analyzer(call, null);
    assertEquals("lower", function.getFuncName());
    assertEquals(1, function.getFuncArgs().size());
    QualifiedName qualified = assertInstanceOf(QualifiedName.class, function.getFuncArgs().get(0));
    assertEquals(List.of("name"), qualified.getParts());
  }

  @Test
  void convertNestedFunction() {
    SqlLiteral first = SqlLiteral.createCharString("a", POS);
    SqlLiteral second = SqlLiteral.createCharString("b", POS);
    SqlBasicCall inner =
        new SqlBasicCall(SqlLibraryOperators.CONCAT_FUNCTION, new SqlNode[] {first, second}, POS);
    SqlBasicCall outer = new SqlBasicCall(SqlStdOperatorTable.LOWER, new SqlNode[] {inner}, POS);

    Function function = analyzer.analyzer(outer, null);
    assertEquals("lower", function.getFuncName());
    Function innerFunc = assertInstanceOf(Function.class, function.getFuncArgs().get(0));
    assertEquals("concat", innerFunc.getFuncName());
    assertEquals(2, innerFunc.getFuncArgs().size());

    Literal arg1 = assertInstanceOf(Literal.class, innerFunc.getFuncArgs().get(0));
    Literal arg2 = assertInstanceOf(Literal.class, innerFunc.getFuncArgs().get(1));
    assertEquals("a", arg1.getValue());
    assertEquals("b", arg2.getValue());
  }

  @Test
  void convertSafeCastFunction() {
    SqlLiteral value = SqlLiteral.createCharString("1", POS);
    SqlTypeNameSpec typeNameSpec = new SqlBasicTypeNameSpec(SqlTypeName.INTEGER, POS);
    SqlDataTypeSpec type = new SqlDataTypeSpec(typeNameSpec, POS);
    SqlBasicCall call =
        new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, new SqlNode[] {value, type}, POS);

    Function function = analyzer.analyzer(call, null);
    assertEquals("safe_cast", function.getFuncName());
    assertEquals(2, function.getFuncArgs().size());

    Literal literalValue = assertInstanceOf(Literal.class, function.getFuncArgs().get(0));
    assertEquals("1", literalValue.getValue());

    Literal typeLiteral = assertInstanceOf(Literal.class, function.getFuncArgs().get(1));
    assertNotNull(typeLiteral.getValue());
    assertEquals("integer", typeLiteral.getValue().toString());
  }

  @Test
  void convertSafeCastDecimalFunction() {
    SqlLiteral value = SqlLiteral.createCharString("1.00", POS);
    SqlTypeNameSpec typeNameSpec = new SqlBasicTypeNameSpec(SqlTypeName.DECIMAL, 10, 2, POS);
    SqlDataTypeSpec type = new SqlDataTypeSpec(typeNameSpec, POS);
    SqlBasicCall call =
        new SqlBasicCall(SqlLibraryOperators.SAFE_CAST, new SqlNode[] {value, type}, POS);

    Function function = analyzer.analyzer(call, null);
    Literal typeLiteral = assertInstanceOf(Literal.class, function.getFuncArgs().get(1));
    assertEquals(DataType.DECIMAL, typeLiteral.getType());
    assertEquals("decimal(10,2)", typeLiteral.getValue());
  }
}
