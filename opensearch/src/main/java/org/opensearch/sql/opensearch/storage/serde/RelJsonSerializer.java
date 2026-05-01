/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.JsonBuilder;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine.OperatorTable;

/**
 * A serializer that (de-)serializes Calcite RexNode, RelDataType and OpenSearch field mapping.
 *
 * <p>This serializer:
 * <li>Uses Calcite's RelJson class to convert RexNode and RelDataType to/from JSON string
 * <li>Manages required OpenSearch field mapping information
 *
 *     <p>Wire format: {@code Base64(UTF-8(<calcite-rel-json-string>))}. The payload is a JSON
 *     string produced by Calcite's {@link RelJson}; it is transported as UTF-8 bytes rather than
 *     wrapped in a Java {@link java.io.ObjectOutputStream} frame to keep the envelope in line with
 *     the JSON-native payload.
 */
@Getter
public class RelJsonSerializer {

  private final RelOptCluster cluster;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF =
      new TypeReference<>() {};
  private static volatile SqlOperatorTable pplSqlOperatorTable;

  static {
    mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
  }

  public RelJsonSerializer(RelOptCluster cluster) {
    this.cluster = cluster;
  }

  private static SqlOperatorTable getPplSqlOperatorTable() {
    if (pplSqlOperatorTable == null) {
      synchronized (RelJsonSerializer.class) {
        if (pplSqlOperatorTable == null) {
          pplSqlOperatorTable =
              SqlOperatorTables.chain(
                  PPLBuiltinOperators.instance(),
                  SqlStdOperatorTable.instance(),
                  OperatorTable.instance(),
                  // Add a list of necessary SqlLibrary if needed
                  SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                      SqlLibrary.MYSQL,
                      SqlLibrary.BIG_QUERY,
                      SqlLibrary.SPARK,
                      SqlLibrary.POSTGRESQL));
        }
      }
    }
    return pplSqlOperatorTable;
  }

  /**
   * Serializes Calcite expressions and field types into a map object string.
   *
   * <p>This method:
   * <li>Standardize the original RexNode
   * <li>Convert RexNode objects to JSON strings.
   * <li>Encodes the resulting map into a final object string
   *
   * @param rexNode pushed down RexNode
   * @return serialized string of RexNode expression.
   */
  public String serialize(RexNode rexNode, ScriptParameterHelper parameterHelper) {
    RexNode standardizedRexExpr =
        RexStandardizer.standardizeRexNodeExpression(rexNode, parameterHelper);
    try {
      // Serialize RexNode and RelDataType by JSON
      JsonBuilder jsonBuilder = new JsonBuilder();
      RelJson relJson = ExtendedRelJson.create(jsonBuilder);
      String rexNodeJson = jsonBuilder.toJsonString(relJson.toJson(standardizedRexExpr));

      if (CalcitePlanContext.skipEncoding.get()) return rexNodeJson;
      // Encode the Calcite JSON string as UTF-8 bytes + Base64.
      //
      // The payload is already a String produced by Calcite's RelJson; wrapping
      // it in ObjectOutputStream writes a Java serialization frame header and
      // extra length metadata that adds no value over raw UTF-8 bytes.
      return Base64.getEncoder()
          .encodeToString(rexNodeJson.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize RexNode: " + standardizedRexExpr, e);
    }
  }

  /**
   * Deserialize serialized map structure string into a map of RexNode, RelDataType and OpenSearch
   * field types.
   *
   * @param struct input serialized map structure string
   * @return map of RexNode, RelDataType and OpenSearch field types
   */
  public RexNode deserialize(String struct) {
    String exprStr = null;
    try {
      // Decode the Calcite JSON string from UTF-8 bytes. Pairs with the
      // serialize() envelope: Base64(UTF-8(<calcite-rel-json-string>)).
      byte[] decoded = Base64.getDecoder().decode(struct);
      exprStr = new String(decoded, StandardCharsets.UTF_8);

      // Deserialize RelDataType and RexNode by JSON
      RelJson relJson = ExtendedRelJson.create((JsonBuilder) null);
      relJson =
          relJson
              .withInputTranslator(ExtendedRelJson::translateInput)
              .withOperatorTable(getPplSqlOperatorTable());
      Map<String, Object> exprMap = mapper.readValue(exprStr, TYPE_REF);
      return relJson.toRex(cluster, exprMap);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize RexNode " + exprStr, e);
    }
  }
}
