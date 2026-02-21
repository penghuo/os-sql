/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.serde;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.JsonBuilder;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine.OperatorTable;
import org.opensearch.sql.opensearch.storage.serde.ExtendedRelJson;

public class RelNodeSerializer {
  private static volatile SqlOperatorTable pplSqlOperatorTable;
  private RelNodeSerializer() {}

  /** Serialize a RelNode subtree to JSON. */
  public static String serialize(RelNode node) {
    try {
      JsonBuilder jsonBuilder = new JsonBuilder();
      RelJsonWriter writer =
          new RelJsonWriter(jsonBuilder, relJson -> ExtendedRelJson.create(jsonBuilder));
      node.explain(writer);
      return writer.asString();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to serialize RelNode: " + node.getRelTypeName(), e);
    }
  }

  /** Deserialize a JSON string back to a RelNode. */
  public static RelNode deserialize(String json, RelOptCluster cluster, RelOptSchema schema) {
    try {
      RelJsonReader reader = new RelJsonReader(cluster, schema, null,
          relJson -> ExtendedRelJson.create((JsonBuilder) null)
              .withInputTranslator(RelNodeSerializer::translateInput)
              .withOperatorTable(getPplSqlOperatorTable()));
      return reader.read(json);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to deserialize RelNode from JSON", e);
    }
  }

  static SqlOperatorTable getPplSqlOperatorTable() {
    if (pplSqlOperatorTable == null) {
      synchronized (RelNodeSerializer.class) {
        if (pplSqlOperatorTable == null) {
          // Order matters: standard Calcite operators first so that standard aggregations
          // (COUNT, SUM, AVG) are found before PPL nullable variants (AVG_NULLABLE etc.)
          // which have the same SqlKind. PPL-only UDFs (JSON_EXTRACT, SPAN, etc.) are
          // not in SqlStdOperatorTable, so they will only be found in PPLBuiltinOperators.
          pplSqlOperatorTable = SqlOperatorTables.chain(
              SqlStdOperatorTable.instance(),
              SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                  SqlLibrary.MYSQL, SqlLibrary.BIG_QUERY, SqlLibrary.SPARK, SqlLibrary.POSTGRESQL),
              PPLBuiltinOperators.instance(),
              OperatorTable.instance());
        }
      }
    }
    return pplSqlOperatorTable;
  }

  @SuppressWarnings("unchecked")
  private static org.apache.calcite.rex.RexNode translateInput(
      RelJson relJson, int input, Map map, org.apache.calcite.rel.RelInput relInput) {
    org.apache.calcite.rex.RexBuilder rexBuilder = relInput.getCluster().getRexBuilder();
    List<RelNode> inputs = relInput.getInputs();
    if (!inputs.isEmpty()) {
      RelNode inputRel = inputs.get(0);
      if (input < inputRel.getRowType().getFieldCount()) {
        return rexBuilder.makeInputRef(
            inputRel.getRowType().getFieldList().get(input).getType(), input);
      }
    }
    if (map.containsKey("type")) {
      org.apache.calcite.rel.type.RelDataType type =
          relJson.toType(relInput.getCluster().getTypeFactory(), map.get("type"));
      return rexBuilder.makeInputRef(type, input);
    }
    throw new IllegalArgumentException("Cannot translate input ref at index " + input);
  }
}
