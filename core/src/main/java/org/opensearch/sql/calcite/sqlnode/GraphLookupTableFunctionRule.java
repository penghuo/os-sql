/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.plan.rel.LogicalGraphLookup;

/**
 * Helper that rewrites a {@link TableFunctionScan} for {@link GraphLookupTableFunction} to {@link
 * LogicalGraphLookup}. Invoked from a custom RelShuttle in {@link SqlNodePlanner} (rather than as a
 * HepPlanner rule) so the parent's input-ref types can be re-derived against the new child's row
 * type.
 */
public final class GraphLookupTableFunctionRule {
  private GraphLookupTableFunctionRule() {}

  public static RelNode rewrite(TableFunctionScan tfs) {
    RexCall rexCall = (RexCall) tfs.getCall();
    List<RexNode> args = rexCall.getOperands();
    List<RelNode> inputs = tfs.getInputs();
    if (inputs.size() != 2) {
      return tfs;
    }
    RelNode source = inputs.get(0);
    RelNode lookup = inputs.get(1);

    // The RexCall's operands hold the SCALAR arguments only — Calcite strips the table operands
    // and turns them into RelNode inputs. Index layout in RexCall.getOperands():
    //   [0] startField (string or null)
    //   [1] fromField, [2] toField, [3] outputField, [4] depthField (string or null)
    //   [5] maxDepth, [6] bidirectional, [7] supportArray, [8] batchMode, [9] usePIT
    //   [10..] startValues (variadic literals, only when literal-start mode)
    String startField = readNullableString(args.get(0));
    String fromField = readString(args.get(1));
    String toField = readString(args.get(2));
    String outputField = readString(args.get(3));
    String depthField = readNullableString(args.get(4));
    int maxDepth = readInt(args.get(5));
    boolean bidirectional = readBoolean(args.get(6));
    boolean supportArray = readBoolean(args.get(7));
    boolean batchMode = readBoolean(args.get(8));
    boolean usePIT = readBoolean(args.get(9));

    List<Object> startValues = null;
    if (startField == null && args.size() > 10) {
      startValues = new java.util.ArrayList<>();
      for (int i = 10; i < args.size(); i++) {
        RexLiteral rl = (RexLiteral) args.get(i);
        // Use getValueAs to unwrap Calcite's internal types (NlsString → String,
        // BigDecimal → expected numeric primitive, etc.) so EnumerableGraphLookup sees
        // the same Java object types v2 produced via PPL Literal.
        Object v = rl.getValueAs(Comparable.class);
        if (v instanceof org.apache.calcite.util.NlsString ns) {
          v = ns.getValue();
        }
        startValues.add(v);
      }
    }

    return LogicalGraphLookup.create(
        source,
        lookup,
        startField,
        startValues,
        fromField,
        toField,
        outputField,
        depthField,
        maxDepth,
        bidirectional,
        supportArray,
        batchMode,
        usePIT,
        /* filter */ null);
  }

  private static String readString(RexNode n) {
    return ((RexLiteral) n).getValueAs(String.class);
  }

  private static String readNullableString(RexNode n) {
    if (n instanceof RexLiteral lit && lit.isNull()) {
      return null;
    }
    return readString(n);
  }

  private static int readInt(RexNode n) {
    Integer v = ((RexLiteral) n).getValueAs(Integer.class);
    return v == null ? 0 : v;
  }

  private static boolean readBoolean(RexNode n) {
    Boolean v = ((RexLiteral) n).getValueAs(Boolean.class);
    return Boolean.TRUE.equals(v);
  }
}
