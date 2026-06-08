/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

/**
 * Subclass of {@link SqlToRelConverter} that handles {@link GraphLookupTableFunction} calls in the
 * FROM clause. Standard {@code convertCollectionTable} converts all operands as scalar expressions,
 * which fails on TABLE-typed operands (treated as SCALAR_QUERY → row type mismatch).
 *
 * <p>We override {@code convertCollectionTable} to:
 *
 * <ol>
 *   <li>Detect the GRAPH_LOOKUP operator
 *   <li>Strip and pre-convert the SET_SEMANTICS_TABLE-wrapped TABLE operands to RelNode inputs
 *   <li>Build the LogicalTableFunctionScan with both inputs and the remaining scalar operands
 * </ol>
 */
public class PplSqlToRelConverter extends SqlToRelConverter {
  public PplSqlToRelConverter(
      RelOptTable.ViewExpander viewExpander,
      SqlValidator validator,
      CatalogReader catalogReader,
      RelOptCluster cluster,
      SqlRexConvertletTable convertletTable,
      Config config) {
    super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
  }

  @Override
  protected void convertCollectionTable(Blackboard bb, SqlCall call) {
    if (call.getOperator() instanceof GraphLookupTableFunction) {
      convertGraphLookupCollectionTable(bb, (SqlBasicCall) call);
      return;
    }
    super.convertCollectionTable(bb, call);
  }

  /**
   * Convert {@code TABLE(GRAPH_LOOKUP(SET_SEMANTICS_TABLE(source), SET_SEMANTICS_TABLE(lookup),
   * scalar...))} to a LogicalTableFunctionScan with the source/lookup converted to RelNode inputs
   * and only scalar args remaining in the RexCall invocation.
   */
  private void convertGraphLookupCollectionTable(Blackboard bb, SqlBasicCall fnCall) {
    GraphLookupTableFunction op = (GraphLookupTableFunction) fnCall.getOperator();
    List<SqlNode> operands = fnCall.getOperandList();
    if (operands.size() < GraphLookupTableFunction.FIXED_PARAM_COUNT) {
      throw new IllegalArgumentException(
          "GRAPH_LOOKUP requires at least "
              + GraphLookupTableFunction.FIXED_PARAM_COUNT
              + " operands; got "
              + operands.size());
    }

    // Convert source (operand 0) — strip SET_SEMANTICS_TABLE wrapper, route through convertFrom.
    RelNode sourceRel = convertTableOperand(bb, operands.get(GraphLookupTableFunction.IDX_SOURCE));
    RelNode lookupRel = convertTableOperand(bb, operands.get(GraphLookupTableFunction.IDX_LOOKUP));

    // Build the scalar arg list (everything from index 2 onward).
    List<SqlNode> scalarOperands = new ArrayList<>();
    for (int i = GraphLookupTableFunction.FIXED_PARAM_COUNT - 10; i < operands.size(); i++) {
      // Skip indices 0,1 (table args) and emit the remaining 10 fixed scalars + variadic.
      // FIXED_PARAM_COUNT-10 = 2, so this loop yields indices 2,3,...,N-1.
      scalarOperands.add(operands.get(i));
    }
    // Convert each scalar operand to a RexNode using the blackboard.
    List<RexNode> scalarRex = new ArrayList<>();
    for (SqlNode s : scalarOperands) {
      scalarRex.add(bb.convertExpression(s));
    }
    // Build a RexCall invocation with ONLY the scalar args.
    RexBuilder rb = getRexBuilder();
    SqlOperator opSql = (SqlOperator) op;
    // Build the LTFS row type by pre-constructing a throwaway LogicalGraphLookup and asking its
    // deriveRowType. This guarantees the LTFS row type matches what GraphLookupTableFunctionRule
    // will produce, so the outer Project (built from `SELECT *`) has inputRef types that align
    // with the rewritten rel — no assertion failures in downstream RelShuttle visits.
    String startField =
        scalarRex.get(0) instanceof org.apache.calcite.rex.RexLiteral lit && lit.getValue() != null
            ? lit.getValueAs(String.class)
            : null;
    String fromField =
        ((org.apache.calcite.rex.RexLiteral) scalarRex.get(1)).getValueAs(String.class);
    String toField =
        ((org.apache.calcite.rex.RexLiteral) scalarRex.get(2)).getValueAs(String.class);
    String outputField =
        ((org.apache.calcite.rex.RexLiteral) scalarRex.get(3)).getValueAs(String.class);
    String depthField = null;
    org.apache.calcite.rex.RexNode depthRex = scalarRex.get(4);
    if (depthRex instanceof org.apache.calcite.rex.RexLiteral dl && dl.getValue() != null) {
      depthField = dl.getValueAs(String.class);
    }
    int maxDepth = ((org.apache.calcite.rex.RexLiteral) scalarRex.get(5)).getValueAs(Integer.class);
    boolean bidirectional =
        Boolean.TRUE.equals(
            ((org.apache.calcite.rex.RexLiteral) scalarRex.get(6)).getValueAs(Boolean.class));
    boolean supportArray =
        Boolean.TRUE.equals(
            ((org.apache.calcite.rex.RexLiteral) scalarRex.get(7)).getValueAs(Boolean.class));
    boolean batchMode =
        Boolean.TRUE.equals(
            ((org.apache.calcite.rex.RexLiteral) scalarRex.get(8)).getValueAs(Boolean.class));
    boolean usePIT =
        Boolean.TRUE.equals(
            ((org.apache.calcite.rex.RexLiteral) scalarRex.get(9)).getValueAs(Boolean.class));
    // startValues = empty list when literal-start mode (startField is null) — LGL.deriveRowType
    // checks `startValues != null` to switch to literal-start row type shape.
    java.util.List<Object> startValuesProbe =
        startField == null ? java.util.Collections.emptyList() : null;
    // Read trailing literal start values from scalarRex past index 9.
    if (startField == null && scalarRex.size() > 10) {
      startValuesProbe = new java.util.ArrayList<>();
      for (int i = 10; i < scalarRex.size(); i++) {
        if (scalarRex.get(i) instanceof org.apache.calcite.rex.RexLiteral lit) {
          startValuesProbe.add(lit.getValue());
        }
      }
    }
    org.opensearch.sql.calcite.plan.rel.LogicalGraphLookup glProbe =
        org.opensearch.sql.calcite.plan.rel.LogicalGraphLookup.create(
            sourceRel,
            lookupRel,
            startField,
            startValuesProbe,
            fromField,
            toField,
            outputField,
            depthField,
            maxDepth,
            bidirectional,
            supportArray,
            batchMode,
            usePIT,
            null);
    org.apache.calcite.rel.type.RelDataType rowType = glProbe.getRowType();
    // Construct a synthetic RexCall: GRAPH_LOOKUP(scalar1, scalar2, ...). The
    // GraphLookupTableFunctionRule reads scalar args from this RexCall's operands.
    RexNode rexInvocation = rb.makeCall(rowType, opSql, scalarRex);

    LogicalTableFunctionScan tfs =
        LogicalTableFunctionScan.create(
            cluster,
            List.of(sourceRel, lookupRel),
            rexInvocation,
            /* elementType */ null,
            rowType,
            /* columnMappings */ null);
    bb.setRoot(tfs, true);
  }

  /**
   * Convert a SET_SEMANTICS_TABLE-wrapped operand to a RelNode. The operand shape is {@code
   * SET_SEMANTICS_TABLE(<query>, <partition>, <order>)}; we use only the inner query.
   */
  private RelNode convertTableOperand(Blackboard bb, SqlNode operand) {
    SqlNode inner = operand;
    if (operand instanceof SqlBasicCall sbc
        && sbc.getOperator() == SqlStdOperatorTable.SET_SEMANTICS_TABLE) {
      inner = sbc.operand(0);
    }
    Blackboard tableBb = createBlackboard(bb.scope(), null, false);
    super.convertFrom(tableBb, inner);
    return tableBb.root();
  }
}
