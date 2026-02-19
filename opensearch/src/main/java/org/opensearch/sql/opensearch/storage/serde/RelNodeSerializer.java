/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableLimitSort;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine.OperatorTable;
import org.opensearch.sql.opensearch.planner.merge.BroadcastExchange;
import org.opensearch.sql.opensearch.planner.merge.HashExchange;
import org.opensearch.sql.opensearch.planner.merge.LocalJoinExchange;
import org.opensearch.sql.opensearch.planner.merge.PartialAggregate;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableTopK;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;

/**
 * Serializes and deserializes full RelNode trees to/from JSON strings.
 *
 * <p>This serializer uses Calcite's {@link RelJsonWriter} for serialization and a custom reader for
 * deserialization. Scan nodes ({@link CalciteEnumerableIndexScan}) are written as placeholder
 * markers and can be rebound to local shard scans during deserialization.
 *
 * <p>Uses {@link ExtendedRelJson} for custom type and RexNode serialization (e.g., UDTs).
 */
public class RelNodeSerializer {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE_REF =
            new TypeReference<>() {};
    private static volatile SqlOperatorTable pplSqlOperatorTable;

    static {
        MAPPER.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
    }

    /** Marker class name used for scan placeholders in serialized JSON. */
    static final String SHARD_SCAN_PLACEHOLDER = "ShardScanPlaceholder";

    /**
     * Serializes a RelNode tree to a JSON string.
     *
     * <p>The output format follows Calcite's {@link RelJsonWriter} conventions: a JSON object with a
     * "rels" array where each entry contains an "id", "relOp", and operator-specific properties.
     *
     * <p>Scan nodes ({@link CalciteEnumerableIndexScan}) are serialized as placeholders with their
     * row type so they can be rebound on the data node.
     *
     * @param root the root of the RelNode tree to serialize
     * @return JSON string representation of the tree
     */
    public String serialize(RelNode root) {
        JsonBuilder jsonBuilder = new JsonBuilder();
        DqeRelJsonWriter writer = new DqeRelJsonWriter(jsonBuilder);
        root.explain(writer);
        return writer.asString();
    }

    /**
     * Deserializes a JSON string back into a RelNode tree.
     *
     * <p>The cluster provides the type factory, Rex builder, and planner context needed to
     * reconstruct the nodes. Scan placeholders in the JSON are reconstructed as {@link
     * ShardScanPlaceholder} nodes that carry the row type for local execution.
     *
     * @param json the JSON string produced by {@link #serialize(RelNode)}
     * @param cluster the RelOptCluster for the target execution environment
     * @return the reconstructed root RelNode
     * @throws IOException if JSON parsing fails
     */
    @SuppressWarnings("unchecked")
    public RelNode deserialize(String json, RelOptCluster cluster) throws IOException {
        Map<String, Object> root = MAPPER.readValue(json, MAP_TYPE_REF);
        List<Map<String, Object>> rels = (List<Map<String, Object>>) root.get("rels");
        if (rels == null || rels.isEmpty()) {
            throw new IllegalArgumentException("No rels found in JSON");
        }

        ExtendedRelJson relJson = createDeserializationRelJson();
        Map<String, RelNode> nodeById = new LinkedHashMap<>();
        String previousId = null;
        RelNode lastNode = null;

        for (Map<String, Object> relMap : rels) {
            String id = (String) relMap.get("id");
            String relOp = (String) relMap.get("relOp");

            // Resolve inputs
            List<RelNode> inputs = resolveInputs(relMap, nodeById, previousId);

            // Construct the node
            RelNode node = constructNode(relOp, relMap, inputs, cluster, relJson);

            nodeById.put(id, node);
            previousId = id;
            lastNode = node;
        }

        return lastNode;
    }

    /**
     * Resolves the input RelNodes for a given rel entry.
     *
     * <p>If the entry has an "inputs" field, uses those IDs. Otherwise, the single input is the
     * immediately previous node (Calcite's convention for linear chains).
     */
    @SuppressWarnings("unchecked")
    private List<RelNode> resolveInputs(
            Map<String, Object> relMap,
            Map<String, RelNode> nodeById,
            @Nullable String previousId) {
        List<String> inputIds = (List<String>) relMap.get("inputs");
        if (inputIds != null) {
            List<RelNode> inputs = new ArrayList<>();
            for (String inputId : inputIds) {
                RelNode input = nodeById.get(inputId);
                if (input == null) {
                    throw new IllegalStateException("Unknown input id: " + inputId);
                }
                inputs.add(input);
            }
            return inputs;
        }
        // No "inputs" field: single input is the previous node (or no inputs for leaf)
        if (previousId != null && nodeById.containsKey(previousId)) {
            return List.of(nodeById.get(previousId));
        }
        return List.of();
    }

    /**
     * Constructs a RelNode from the JSON properties based on the operator type.
     *
     * @param relOp the operator class name
     * @param props the JSON properties map
     * @param inputs the resolved input RelNodes
     * @param cluster the target cluster
     * @param relJson the JSON helper for RexNode/type deserialization
     * @return the constructed RelNode
     */
    @SuppressWarnings("unchecked")
    private RelNode constructNode(
            String relOp,
            Map<String, Object> props,
            List<RelNode> inputs,
            RelOptCluster cluster,
            ExtendedRelJson relJson) {
        RelTraitSet enumerableTraits = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        RelNode input = inputs.isEmpty() ? null : inputs.get(0);

        String simpleName = simplifyClassName(relOp);
        switch (simpleName) {
            case SHARD_SCAN_PLACEHOLDER:
                return constructShardScanPlaceholder(props, cluster, enumerableTraits, relJson);

            case "EnumerableFilter":
                return constructFilter(props, input, cluster, enumerableTraits, relJson);

            case "EnumerableProject":
                return constructProject(props, input, cluster, enumerableTraits, relJson);

            case "EnumerableSort":
                return constructSort(props, input, cluster, enumerableTraits, relJson);

            case "EnumerableLimitSort":
                return constructLimitSort(props, input, cluster, enumerableTraits, relJson);

            case "CalciteEnumerableTopK":
                return constructTopK(props, input, cluster, enumerableTraits, relJson);

            case "EnumerableAggregate":
                return constructAggregate(props, input, cluster, enumerableTraits, relJson);

            case "EnumerableLimit":
                return constructLimit(props, input, cluster, enumerableTraits, relJson);

            case "BroadcastExchange":
                return new BroadcastExchange(cluster, enumerableTraits, input);

            case "HashExchange":
                return constructHashExchange(props, input, cluster, enumerableTraits);

            case "LocalJoinExchange":
                return constructLocalJoinExchange(props, input, cluster, enumerableTraits, relJson);

            case "PartialAggregate":
                return constructPartialAggregate(props, input, cluster, enumerableTraits, relJson);

            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator for deserialization: " + relOp);
        }
    }

    private ShardScanPlaceholder constructShardScanPlaceholder(
            Map<String, Object> props,
            RelOptCluster cluster,
            RelTraitSet traits,
            ExtendedRelJson relJson) {
        Object rowTypeJson = props.get("rowType");
        RelDataType rowType = relJson.toType(cluster.getTypeFactory(), rowTypeJson);
        return new ShardScanPlaceholder(cluster, traits, rowType);
    }

    private EnumerableFilter constructFilter(
            Map<String, Object> props,
            RelNode input,
            RelOptCluster cluster,
            RelTraitSet traits,
            ExtendedRelJson relJson) {
        RexNode condition = toRex(relJson, cluster, input, props.get("condition"));
        return new EnumerableFilter(cluster, traits, input, condition);
    }

    @SuppressWarnings("unchecked")
    private EnumerableProject constructProject(
            Map<String, Object> props,
            RelNode input,
            RelOptCluster cluster,
            RelTraitSet traits,
            ExtendedRelJson relJson) {
        List<Object> exprJsonList = (List<Object>) props.get("exprs");
        List<RexNode> exprs = new ArrayList<>();
        for (Object exprJson : exprJsonList) {
            exprs.add(toRex(relJson, cluster, input, exprJson));
        }
        List<String> fields = (List<String>) props.get("fields");
        RelDataType rowType = deriveProjectRowType(cluster.getTypeFactory(), exprs, fields);
        return new EnumerableProject(cluster, traits, input, exprs, rowType);
    }

    @SuppressWarnings("unchecked")
    private EnumerableSort constructSort(
            Map<String, Object> props,
            RelNode input,
            RelOptCluster cluster,
            RelTraitSet traits,
            ExtendedRelJson relJson) {
        RelCollation collation = relJson.toCollation((List<Map<String, Object>>) props.get("collation"));
        // EnumerableSort does not support fetch/offset - use EnumerableLimitSort for that
        return new EnumerableSort(cluster, traits.replace(collation), input, collation, null, null);
    }

    @SuppressWarnings("unchecked")
    private EnumerableLimitSort constructLimitSort(
            Map<String, Object> props,
            RelNode input,
            RelOptCluster cluster,
            RelTraitSet traits,
            ExtendedRelJson relJson) {
        RelCollation collation = relJson.toCollation((List<Map<String, Object>>) props.get("collation"));
        RexNode offset = props.containsKey("offset") ? toRex(relJson, cluster, input, props.get("offset")) : null;
        RexNode fetch = props.containsKey("fetch") ? toRex(relJson, cluster, input, props.get("fetch")) : null;
        return new EnumerableLimitSort(cluster, traits.replace(collation), input, collation, offset, fetch);
    }

    @SuppressWarnings("unchecked")
    private CalciteEnumerableTopK constructTopK(
            Map<String, Object> props,
            RelNode input,
            RelOptCluster cluster,
            RelTraitSet traits,
            ExtendedRelJson relJson) {
        RelCollation collation = relJson.toCollation((List<Map<String, Object>>) props.get("collation"));
        RexNode offset = props.containsKey("offset") ? toRex(relJson, cluster, input, props.get("offset")) : null;
        RexNode fetch = props.containsKey("fetch") ? toRex(relJson, cluster, input, props.get("fetch")) : null;
        return new CalciteEnumerableTopK(cluster, traits.replace(collation), input, collation, offset, fetch);
    }

    @SuppressWarnings("unchecked")
    private EnumerableAggregate constructAggregate(
            Map<String, Object> props,
            RelNode input,
            RelOptCluster cluster,
            RelTraitSet traits,
            ExtendedRelJson relJson) {
        ImmutableBitSet groupSet = toBitSet((List<Number>) props.get("group"));
        List<ImmutableBitSet> groupSets = null;
        if (props.containsKey("groups")) {
            groupSets = new ArrayList<>();
            for (List<Number> group : (List<List<Number>>) props.get("groups")) {
                groupSets.add(toBitSet(group));
            }
        }
        List<AggregateCall> aggCalls =
                toAggCalls(
                        (List<Map<String, Object>>) props.get("aggs"),
                        input,
                        cluster,
                        relJson);
        try {
            return new EnumerableAggregate(
                    cluster, traits, input, groupSet, groupSets, aggCalls);
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Failed to construct EnumerableAggregate", e);
        }
    }

    private EnumerableLimit constructLimit(
            Map<String, Object> props,
            RelNode input,
            RelOptCluster cluster,
            RelTraitSet traits,
            ExtendedRelJson relJson) {
        RexNode offset = props.containsKey("offset") ? toRex(relJson, cluster, input, props.get("offset")) : null;
        RexNode fetch = props.containsKey("fetch") ? toRex(relJson, cluster, input, props.get("fetch")) : null;
        return EnumerableLimit.create(input, offset, fetch);
    }

    @SuppressWarnings("unchecked")
    private HashExchange constructHashExchange(
            Map<String, Object> props,
            RelNode input,
            RelOptCluster cluster,
            RelTraitSet traits) {
        List<Number> distKeys = (List<Number>) props.get("distributionKeys");
        ImmutableList.Builder<Integer> keysBuilder = ImmutableList.builder();
        for (Number key : distKeys) {
            keysBuilder.add(key.intValue());
        }
        int numPartitions = ((Number) props.get("numPartitions")).intValue();
        return new HashExchange(
                cluster, traits, input, keysBuilder.build(), numPartitions);
    }

    private LocalJoinExchange constructLocalJoinExchange(
            Map<String, Object> props,
            RelNode input,
            RelOptCluster cluster,
            RelTraitSet traits,
            ExtendedRelJson relJson) {
        JoinRelType joinType = JoinRelType.valueOf((String) props.get("joinType"));
        RexNode condition = toRex(relJson, cluster, input, props.get("condition"));
        return new LocalJoinExchange(cluster, traits, input, joinType, condition);
    }

    @SuppressWarnings("unchecked")
    private PartialAggregate constructPartialAggregate(
            Map<String, Object> props,
            RelNode input,
            RelOptCluster cluster,
            RelTraitSet traits,
            ExtendedRelJson relJson) {
        ImmutableBitSet groupSet = toBitSet((List<Number>) props.get("group"));
        List<ImmutableBitSet> groupSets = null;
        if (props.containsKey("groups")) {
            groupSets = new ArrayList<>();
            for (List<Number> group : (List<List<Number>>) props.get("groups")) {
                groupSets.add(toBitSet(group));
            }
        }
        List<AggregateCall> aggCalls =
                toAggCalls(
                        (List<Map<String, Object>>) props.get("aggs"),
                        input,
                        cluster,
                        relJson);
        // PartialAggregate extends Aggregate, reconstruct via copy
        try {
            EnumerableAggregate tempAgg =
                    new EnumerableAggregate(cluster, traits, input, groupSet, groupSets, aggCalls);
            PartialAggregate partial = PartialAggregate.create(tempAgg);
            if (partial != null) {
                return partial;
            }
            // If create returns null (shouldn't happen for deserialized partials), fall through
            throw new IllegalStateException("Cannot reconstruct PartialAggregate from serialized data");
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Failed to construct PartialAggregate", e);
        }
    }

    /**
     * Converts a list of aggregate call JSON maps back to AggregateCall objects.
     *
     * <p>This mirrors the format produced by {@link RelJson#toJson(AggregateCall)} and matches the
     * deserialization logic in Calcite's RelJsonReader.
     */
    @SuppressWarnings("unchecked")
    private List<AggregateCall> toAggCalls(
            List<Map<String, Object>> aggJsonList,
            RelNode input,
            RelOptCluster cluster,
            ExtendedRelJson relJson) {
        List<AggregateCall> calls = new ArrayList<>();
        for (Map<String, Object> aggMap : aggJsonList) {
            Map<String, Object> aggJson = (Map<String, Object>) aggMap.get("agg");
            String aggName = (String) aggJson.get("name");
            org.apache.calcite.sql.SqlAggFunction aggFunc = relJson.toAggregation(aggJson);

            Boolean distinct = (Boolean) aggMap.get("distinct");
            List<Integer> operandList = (List<Integer>) aggMap.get("operands");
            ImmutableList<Integer> operands =
                    operandList != null ? ImmutableList.copyOf(operandList) : ImmutableList.of();

            Object typeJson = aggMap.get("type");
            RelDataType type = relJson.toType(cluster.getTypeFactory(), typeJson);

            String name = (String) aggMap.get("name");

            int filterArg = aggMap.containsKey("filter") ? ((Number) aggMap.get("filter")).intValue() : -1;

            RelCollation collation = RelCollations.EMPTY;

            calls.add(
                    AggregateCall.create(
                            aggFunc,
                            distinct != null && distinct,
                            false,
                            false,
                            ImmutableList.of(),
                            operands,
                            filterArg,
                            null,
                            collation,
                            type,
                            name));
        }
        return calls;
    }

    /** Converts a JSON list of integers to an ImmutableBitSet. */
    private static ImmutableBitSet toBitSet(List<Number> list) {
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (Number n : list) {
            builder.set(n.intValue());
        }
        return builder.build();
    }

    /** Derives the row type for a Project node from its expressions and field names. */
    private RelDataType deriveProjectRowType(
            RelDataTypeFactory typeFactory, List<RexNode> exprs, List<String> fields) {
        List<RelDataType> types = new ArrayList<>();
        for (RexNode expr : exprs) {
            types.add(expr.getType());
        }
        return typeFactory.createStructType(types, fields);
    }

    /** Extracts the simple class name from a potentially fully-qualified class name. */
    private String simplifyClassName(String relOp) {
        int lastDot = relOp.lastIndexOf('.');
        String simplified = lastDot >= 0 ? relOp.substring(lastDot + 1) : relOp;
        // Handle inner classes (e.g., "RelNodeSerializer$ShardScanPlaceholder")
        int dollarSign = simplified.lastIndexOf('$');
        return dollarSign >= 0 ? simplified.substring(dollarSign + 1) : simplified;
    }

    /**
     * Deserializes a RexNode from JSON using the given relJson helper and a proper RelInput context
     * that allows RexInputRef resolution from the input node's row type.
     */
    private RexNode toRex(
            ExtendedRelJson relJson,
            RelOptCluster cluster,
            @Nullable RelNode inputNode,
            Object jsonObj) {
        DeserializationRelInput relInput = new DeserializationRelInput(cluster, inputNode);
        return relJson.toRex(relInput, jsonObj);
    }

    /** Creates an ExtendedRelJson configured for deserialization with PPL operator support. */
    private ExtendedRelJson createDeserializationRelJson() {
        ExtendedRelJson relJson = ExtendedRelJson.create((JsonBuilder) null);
        // Use a proper input translator that resolves RexInputRef from the input node's row type
        RelJson.InputTranslator inputTranslator =
                (rj, input, map, relInput) -> {
                    List<RelNode> inputNodes = relInput.getInputs();
                    if (inputNodes.isEmpty()) {
                        throw new IllegalStateException(
                                "Cannot resolve RexInputRef: no input nodes available");
                    }
                    return relInput.getCluster()
                            .getRexBuilder()
                            .makeInputRef(inputNodes.get(0), input);
                };
        relJson =
                (ExtendedRelJson)
                        relJson.withInputTranslator(inputTranslator)
                                .withOperatorTable(getPplSqlOperatorTable());
        return relJson;
    }

    private static SqlOperatorTable getPplSqlOperatorTable() {
        if (pplSqlOperatorTable == null) {
            synchronized (RelNodeSerializer.class) {
                if (pplSqlOperatorTable == null) {
                    pplSqlOperatorTable =
                            SqlOperatorTables.chain(
                                    PPLBuiltinOperators.instance(),
                                    SqlStdOperatorTable.instance(),
                                    OperatorTable.instance(),
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
     * Custom RelJsonWriter that handles scan nodes by writing them as shard scan placeholders with
     * row type information instead of table references.
     */
    private static class DqeRelJsonWriter extends RelJsonWriter {
        private final JsonBuilder jsonBuilder;
        private final ExtendedRelJson extRelJson;

        DqeRelJsonWriter(JsonBuilder jsonBuilder) {
            super(jsonBuilder, relJson -> ExtendedRelJson.create(jsonBuilder));
            this.jsonBuilder = jsonBuilder;
            this.extRelJson = ExtendedRelJson.create(jsonBuilder);
        }

        @Override
        protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
            if (rel instanceof CalciteEnumerableIndexScan
                    || rel instanceof ShardScanPlaceholder) {
                // Write scan as a placeholder with row type
                Map<String, Object> map = jsonBuilder.map();
                map.put("relOp", SHARD_SCAN_PLACEHOLDER);
                map.put("rowType", extRelJson.toJson(rel.getRowType()));
                String id = Integer.toString(relList.size());
                map.put("id", id);
                relList.add(map);
            } else {
                super.explain_(rel, values);
            }
        }
    }

    /**
     * Minimal RelInput implementation for RexNode deserialization. Provides the cluster and input
     * nodes needed by the InputTranslator to resolve RexInputRef.
     */
    private static class DeserializationRelInput implements org.apache.calcite.rel.RelInput {
        private final RelOptCluster cluster;
        private final @Nullable RelNode inputNode;

        DeserializationRelInput(RelOptCluster cluster, @Nullable RelNode inputNode) {
            this.cluster = cluster;
            this.inputNode = inputNode;
        }

        @Override
        public RelOptCluster getCluster() {
            return cluster;
        }

        @Override
        public List<RelNode> getInputs() {
            return inputNode != null ? List.of(inputNode) : List.of();
        }

        @Override
        public RelTraitSet getTraitSet() {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.calcite.plan.RelOptTable getTable(String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RelNode getInput() {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable RexNode getExpression(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ImmutableBitSet getBitSet(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable List<ImmutableBitSet> getBitSetList(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<AggregateCall> getAggregateCalls(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable Object get(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable String getString(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.math.BigDecimal getBigDecimal(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <E extends Enum<E>> @Nullable E getEnum(String tag, Class<E> enumClass) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable List<RexNode> getExpressionList(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable List<String> getStringList(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable List<Integer> getIntegerList(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable List<List<Integer>> getIntegerListList(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RelDataType getRowType(String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RelDataType getRowType(String expressionsTag, String fieldsTag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RelCollation getCollation() {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.calcite.rel.RelDistribution getDistribution() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ImmutableList<ImmutableList<org.apache.calcite.rex.RexLiteral>> getTuples(
                String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBoolean(String tag, boolean default_) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Placeholder RelNode that represents a scan node in a serialized shard fragment. It carries the
     * row type so the shard-side runtime can replace it with an actual local shard scan operator.
     */
    public static class ShardScanPlaceholder extends org.apache.calcite.rel.AbstractRelNode {

        private final RelDataType scanRowType;

        public ShardScanPlaceholder(
                RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType) {
            super(cluster, traitSet);
            this.scanRowType = rowType;
        }

        @Override
        protected RelDataType deriveRowType() {
            return scanRowType;
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("rowType", scanRowType);
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new ShardScanPlaceholder(getCluster(), traitSet, scanRowType);
        }
    }
}
