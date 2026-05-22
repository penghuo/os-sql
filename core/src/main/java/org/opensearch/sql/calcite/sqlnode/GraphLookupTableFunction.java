/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.TableCharacteristic;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Polymorphic Table Function (PTF) for PPL's graphLookup.
 *
 * <p>Calling shape:
 *
 * <pre>
 *   TABLE(GRAPH_LOOKUP(
 *     TABLE source,         -- in-flight pipeline rows
 *     TABLE lookup,          -- lookup table (with optional filter)
 *     'startField',          -- string, nullable for literal-start mode
 *     'startValue1', ...,    -- variadic literals for literal-start mode (string/int/etc.)
 *     'fromField',           -- string
 *     'toField',             -- string
 *     'outputField',         -- string
 *     'depthField',          -- string, nullable
 *     maxDepth,              -- int
 *     bidirectional,         -- boolean
 *     supportArray,          -- boolean
 *     batchMode,             -- boolean
 *     usePIT))               -- boolean
 * </pre>
 *
 * <p>The validator computes the row type via {@link #getRowTypeInference()}, which inspects the
 * input table types to derive the output schema (matching {@link
 * org.opensearch.sql.calcite.plan.rel.LogicalGraphLookup#deriveRowType()}).
 *
 * <p>A planner rule rewrites {@code LogicalTableFunctionScan(GRAPH_LOOKUP(...))} to {@link
 * org.opensearch.sql.calcite.plan.rel.LogicalGraphLookup} so the existing OpenSearch storage rule
 * picks it up.
 */
public class GraphLookupTableFunction extends SqlFunction implements SqlTableFunction {
  public static final String NAME = "GRAPH_LOOKUP";
  public static final GraphLookupTableFunction INSTANCE = new GraphLookupTableFunction();

  // Param positions (zero-indexed). Matches the calling shape above.
  public static final int IDX_SOURCE = 0;
  public static final int IDX_LOOKUP = 1;
  public static final int IDX_START_FIELD = 2;
  public static final int IDX_FROM_FIELD = 3;
  public static final int IDX_TO_FIELD = 4;
  public static final int IDX_OUTPUT_FIELD = 5;
  public static final int IDX_DEPTH_FIELD = 6;
  public static final int IDX_MAX_DEPTH = 7;
  public static final int IDX_BIDIRECTIONAL = 8;
  public static final int IDX_SUPPORT_ARRAY = 9;
  public static final int IDX_BATCH_MODE = 10;
  public static final int IDX_USE_PIT = 11;
  public static final int FIXED_PARAM_COUNT = 12;

  private GraphLookupTableFunction() {
    super(
        new SqlIdentifier(NAME, SqlParserPos.ZERO),
        /* returnTypeInference */ org.apache.calcite.sql.type.ReturnTypes.CURSOR,
        /* operandTypeInference */ null,
        new GraphLookupOperandMetadata(),
        /* paramTypes */ null,
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
  }

  @Override
  public SqlReturnTypeInference getRowTypeInference() {
    return GraphLookupTableFunction::inferRowType;
  }

  @Override
  public TableCharacteristic tableCharacteristic(int ordinal) {
    if (ordinal == IDX_SOURCE || ordinal == IDX_LOOKUP) {
      return TableCharacteristic.builder(TableCharacteristic.Semantics.SET).build();
    }
    return null;
  }

  /**
   * Compute the output row type from operand types, mirroring {@link
   * org.opensearch.sql.calcite.plan.rel.LogicalGraphLookup#deriveRowType()}.
   */
  private static RelDataType inferRowType(SqlOperatorBinding binding) {
    RelDataTypeFactory tf = binding.getTypeFactory();
    RelDataType sourceRow = binding.getOperandType(IDX_SOURCE);
    RelDataType lookupRow = binding.getOperandType(IDX_LOOKUP);
    String outputField = readStringLiteral(binding, IDX_OUTPUT_FIELD);
    String depthField = tryReadStringLiteral(binding, IDX_DEPTH_FIELD);
    boolean batchMode = readBooleanLiteral(binding, IDX_BATCH_MODE);
    boolean literalStart = !hasOperand(binding, IDX_START_FIELD);

    // Match LogicalGraphLookup.deriveRowType exactly: lookup row is NOT NULL when inside the
    // ARRAY. Force NOT NULL on the inner element to align with what the SqlToRelConverter
    // produces — without this, the GraphLookupTableFunctionRule rewrite to LogicalGraphLookup
    // fails the type-equality check.
    RelDataType lookupRowWithDepth = lookupRow;
    if (depthField != null) {
      RelDataTypeFactory.Builder lb = tf.builder();
      lb.addAll(lookupRow.getFieldList());
      lb.add(depthField, tf.createSqlType(SqlTypeName.INTEGER));
      lookupRowWithDepth = lb.build();
    }
    RelDataType notNullLookupRow = tf.createTypeWithNullability(lookupRowWithDepth, false);
    RelDataType notNullSourceRow = tf.createTypeWithNullability(sourceRow, false);

    RelDataTypeFactory.Builder builder = tf.builder();
    if (literalStart) {
      builder.add(outputField, tf.createArrayType(notNullLookupRow, -1));
    } else if (batchMode) {
      String startField = readStringLiteral(binding, IDX_START_FIELD);
      builder.add(startField, tf.createArrayType(notNullSourceRow, -1));
      builder.add(outputField, tf.createArrayType(notNullLookupRow, -1));
    } else {
      builder.addAll(sourceRow.getFieldList());
      builder.add(outputField, tf.createArrayType(notNullLookupRow, -1));
    }
    return builder.build();
  }

  private static boolean hasOperand(SqlOperatorBinding b, int idx) {
    if (idx >= b.getOperandCount()) return false;
    try {
      RelDataType t = b.getOperandType(idx);
      return t != null && t.getSqlTypeName() != SqlTypeName.NULL;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static String readStringLiteral(SqlOperatorBinding b, int idx) {
    try {
      return b.getStringLiteralOperand(idx);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static String tryReadStringLiteral(SqlOperatorBinding b, int idx) {
    if (!hasOperand(b, idx)) return null;
    return readStringLiteral(b, idx);
  }

  private static boolean readBooleanLiteral(SqlOperatorBinding b, int idx) {
    if (b instanceof SqlCallBinding scb) {
      Boolean v = scb.getOperandLiteralValue(idx, Boolean.class);
      return Boolean.TRUE.equals(v);
    }
    return false;
  }

  /** Operand metadata: 12 fixed positions, mostly accept-anything. */
  private static class GraphLookupOperandMetadata implements SqlOperandMetadata {
    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
      // All operands except the two TABLE operands are scalar; we accept ANY for flexibility.
      List<RelDataType> types = new ArrayList<>();
      for (int i = 0; i < FIXED_PARAM_COUNT; i++) {
        types.add(typeFactory.createSqlType(SqlTypeName.ANY));
      }
      return types;
    }

    @Override
    public List<String> paramNames() {
      return List.of(
          "source",
          "lookup",
          "startField",
          "fromField",
          "toField",
          "outputField",
          "depthField",
          "maxDepth",
          "bidirectional",
          "supportArray",
          "batchMode",
          "usePIT");
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      // Permissive: graphLookup operand validation happens implicitly via the table-function
      // tableCharacteristic + scalar literal resolution.
      return callBinding.getOperandCount() >= FIXED_PARAM_COUNT;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      // 12 fixed + arbitrary trailing literals (literal-start mode appends startValues).
      return SqlOperandCountRanges.from(FIXED_PARAM_COUNT);
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + "(<TABLE source>, <TABLE lookup>, <STRING ...>)";
    }

    @Override
    public Consistency getConsistency() {
      return Consistency.NONE;
    }

    @Override
    public boolean isOptional(int i) {
      // depthField (idx 6) and startField (idx 2) can be null literals.
      return i == IDX_START_FIELD || i == IDX_DEPTH_FIELD;
    }
  }
}
