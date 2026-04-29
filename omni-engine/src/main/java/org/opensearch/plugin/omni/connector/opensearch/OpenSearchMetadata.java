/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.*;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.RealType;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;

import java.util.*;

import static java.util.Objects.requireNonNull;

public class OpenSearchMetadata
        implements ConnectorMetadata
{
    private static final String DEFAULT_SCHEMA = "default";
    private final ClusterService clusterService;

    public OpenSearchMetadata(ClusterService clusterService)
    {
        this.clusterService = requireNonNull(clusterService);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(DEFAULT_SCHEMA);
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return DEFAULT_SCHEMA.equals(schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (!DEFAULT_SCHEMA.equals(tableName.getSchemaName())) {
            return null;
        }
        String pattern = tableName.getTableName();

        // Filter system indices (literal names starting with "." that aren't patterns)
        if (pattern.startsWith(".") && !pattern.contains("*") && !pattern.contains(",")) {
            return null;
        }

        ClusterState state = clusterService.state();

        // Resolve the pattern to concrete indices
        org.opensearch.cluster.metadata.IndexNameExpressionResolver resolver =
                new org.opensearch.cluster.metadata.IndexNameExpressionResolver(
                        new org.opensearch.common.util.concurrent.ThreadContext(
                                org.opensearch.common.settings.Settings.EMPTY));

        String[] patterns = pattern.split(",");
        for (int i = 0; i < patterns.length; i++) {
            patterns[i] = patterns[i].trim();
        }
        String[] concreteIndices;
        try {
            concreteIndices = resolver.concreteIndexNames(
                    state,
                    org.opensearch.action.support.IndicesOptions.lenientExpandOpen(),
                    patterns);
        } catch (Exception e) {
            return null; // Pattern didn't resolve
        }

        if (concreteIndices.length == 0) {
            return null;
        }

        // Filter out system indices and closed indices
        List<String> resolved = new ArrayList<>();
        for (String idx : concreteIndices) {
            if (idx.startsWith(".")) {
                continue; // Skip system index
            }
            IndexMetadata md = state.metadata().index(idx);
            if (md != null && md.getState() != IndexMetadata.State.CLOSE) {
                resolved.add(idx);
            }
        }

        if (resolved.isEmpty()) {
            return null;
        }

        return new OpenSearchTableHandle(pattern, resolved);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        OpenSearchTableHandle osTable = (OpenSearchTableHandle) table;
        List<OpenSearchColumnHandle> columns = getColumnsForIndices(osTable.getResolvedIndices());
        List<ColumnMetadata> columnMetadatas = new ArrayList<>();
        for (OpenSearchColumnHandle col : columns) {
            columnMetadatas.add(new ColumnMetadata(col.getName(), col.getType()));
        }
        // Add built-in hidden columns
        columnMetadatas.add(ColumnMetadata.builder().setName("_id").setType(VarcharType.VARCHAR).setHidden(true).build());
        columnMetadatas.add(ColumnMetadata.builder().setName("_source").setType(VarcharType.VARCHAR).setHidden(true).build());
        columnMetadatas.add(ColumnMetadata.builder().setName("_score").setType(RealType.REAL).setHidden(true).build());
        return new ConnectorTableMetadata(
                new SchemaTableName(DEFAULT_SCHEMA, osTable.getPattern()),
                columnMetadatas);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        OpenSearchTableHandle osTable = (OpenSearchTableHandle) table;
        List<OpenSearchColumnHandle> columns = getColumnsForIndices(osTable.getResolvedIndices());
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (OpenSearchColumnHandle col : columns) {
            builder.put(col.getName(), col);
        }
        // Built-in columns
        builder.put("_id", new OpenSearchColumnHandle("_id", VarcharType.VARCHAR, false));
        builder.put("_source", new OpenSearchColumnHandle("_source", VarcharType.VARCHAR, false));
        builder.put("_score", new OpenSearchColumnHandle("_score", RealType.REAL, false));
        return builder.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        OpenSearchColumnHandle col = (OpenSearchColumnHandle) columnHandle;
        boolean hidden = col.getName().startsWith("_");
        return ColumnMetadata.builder()
                .setName(col.getName())
                .setType(col.getType())
                .setHidden(hidden)
                .build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && !DEFAULT_SCHEMA.equals(schemaName.get())) {
            return ImmutableList.of();
        }
        ClusterState state = clusterService.state();
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (var entry : state.metadata().indices().entrySet()) {
            String indexName = entry.getKey();
            if (!indexName.startsWith(".") && entry.getValue().getState() != IndexMetadata.State.CLOSE) {
                tables.add(new SchemaTableName(DEFAULT_SCHEMA, indexName));
            }
        }
        return tables.build();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        OpenSearchTableHandle table = (OpenSearchTableHandle) handle;
        TupleDomain<ColumnHandle> newConstraint = constraint.getSummary();

        // Separate supported vs unsupported predicates
        Map<OpenSearchColumnHandle, Domain> supported = new LinkedHashMap<>();
        Map<OpenSearchColumnHandle, Domain> unsupported = new LinkedHashMap<>();

        if (newConstraint.getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : newConstraint.getDomains().get().entrySet()) {
                OpenSearchColumnHandle col = (OpenSearchColumnHandle) entry.getKey();
                if (col.isSupportsPredicates()) {
                    supported.put(col, entry.getValue());
                } else {
                    unsupported.put(col, entry.getValue());
                }
            }
        }

        // Intersect with existing constraint (not replace!)
        TupleDomain<OpenSearchColumnHandle> newPushed = TupleDomain.withColumnDomains(supported)
                .intersect(table.getConstraint());

        // Extract LIKE patterns from ConnectorExpression
        Map<String, String> newLikePatterns = new LinkedHashMap<>(table.getLikePatterns());
        ConnectorExpression expression = constraint.getExpression();
        ConnectorExpression remainingExpression = extractLikePatterns(
                expression, constraint.getAssignments(), newLikePatterns);

        // Extract query_string expression from ConnectorExpression
        Optional<String> newQueryString = table.getQueryStringExpression();
        var queryStringResult = extractQueryString(remainingExpression);
        if (queryStringResult.queryString.isPresent()) {
            newQueryString = queryStringResult.queryString;
            remainingExpression = queryStringResult.remaining;
        }

        // Return empty if nothing changed
        if (newPushed.equals(table.getConstraint())
                && newLikePatterns.equals(table.getLikePatterns())
                && newQueryString.equals(table.getQueryStringExpression())) {
            return Optional.empty();
        }

        OpenSearchTableHandle newTable = new OpenSearchTableHandle(
                table.getPattern(), table.getResolvedIndices(), newPushed, table.getLimit(), newLikePatterns, newQueryString);

        return Optional.of(new ConstraintApplicationResult<>(
                newTable,
                TupleDomain.withColumnDomains(new LinkedHashMap<>(unsupported)),
                remainingExpression,
                false));
    }

    /**
     * Extract LIKE patterns from the ConnectorExpression tree.
     * Handles: $like(column, pattern) and $and($like(...), $like(...), ...)
     * Returns the remaining (non-LIKE) expression.
     */
    private ConnectorExpression extractLikePatterns(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            Map<String, String> likePatterns)
    {
        if (expression instanceof Call call) {
            // Handle $like(column, pattern)
            if (call.getFunctionName().equals(StandardFunctions.LIKE_FUNCTION_NAME)
                    && call.getArguments().size() == 2) {
                ConnectorExpression columnExpr = call.getArguments().get(0);
                ConnectorExpression patternExpr = call.getArguments().get(1);

                if (columnExpr instanceof Variable variable && patternExpr instanceof Constant constant) {
                    ColumnHandle colHandle = assignments.get(variable.getName());
                    if (colHandle instanceof OpenSearchColumnHandle osCol
                            && osCol.isSupportsPredicates()) {
                        Object value = constant.getValue();
                        if (value instanceof io.airlift.slice.Slice slice) {
                            String pattern = slice.toStringUtf8();
                            likePatterns.put(osCol.getOpensearchName(), pattern);
                            // Return TRUE — we consumed this expression
                            return Constant.TRUE;
                        }
                    }
                }
            }

            // Handle $and(expr1, expr2, ...) — extract LIKE from each branch
            if (call.getFunctionName().equals(StandardFunctions.AND_FUNCTION_NAME)) {
                List<ConnectorExpression> remaining = new java.util.ArrayList<>();
                boolean anyExtracted = false;
                for (ConnectorExpression arg : call.getArguments()) {
                    ConnectorExpression result = extractLikePatterns(arg, assignments, likePatterns);
                    if (result.equals(Constant.TRUE)) {
                        anyExtracted = true;
                    } else {
                        remaining.add(result);
                    }
                }
                if (anyExtracted) {
                    if (remaining.isEmpty()) {
                        return Constant.TRUE;
                    }
                    if (remaining.size() == 1) {
                        return remaining.get(0);
                    }
                    // Reconstruct AND with remaining expressions
                    return new Call(call.getType(), call.getFunctionName(), remaining);
                }
            }
        }

        // Can't extract — return as-is
        return expression;
    }

    private record QueryStringExtraction(Optional<String> queryString, ConnectorExpression remaining) {}

    /**
     * Extract query_string(literal) from ConnectorExpression.
     * Handles: query_string('...') as a standalone call or inside $and(...).
     */
    private QueryStringExtraction extractQueryString(ConnectorExpression expression)
    {
        if (expression instanceof Call call) {
            // Handle query_string('...') or query_string(MAP('query', '...'))
            if (call.getFunctionName().getCatalogSchema().isEmpty()
                    && call.getFunctionName().getName().equals("query_string")
                    && call.getArguments().size() == 1) {
                ConnectorExpression arg = call.getArguments().get(0);
                // Direct varchar literal: query_string('...')
                if (arg instanceof Constant constant && constant.getValue() instanceof io.airlift.slice.Slice slice) {
                    return new QueryStringExtraction(Optional.of(slice.toStringUtf8()), Constant.TRUE);
                }
                // MAP literal: query_string(MAP('query', '...'))
                // The ConnectorExpression for MAP is opaque — extract from the SQL string instead.
                // For now, we can't easily extract MAP values from ConnectorExpression.
                // Fall through to let the UDF execute (returns true = no filtering).
            }

            // Handle $and(expr1, expr2, ...) — extract query_string from branches
            if (call.getFunctionName().equals(StandardFunctions.AND_FUNCTION_NAME)) {
                List<ConnectorExpression> remaining = new java.util.ArrayList<>();
                Optional<String> found = Optional.empty();
                for (ConnectorExpression arg : call.getArguments()) {
                    var result = extractQueryString(arg);
                    if (result.queryString.isPresent()) {
                        found = result.queryString;
                        if (!result.remaining.equals(Constant.TRUE)) {
                            remaining.add(result.remaining);
                        }
                    } else {
                        remaining.add(arg);
                    }
                }
                if (found.isPresent()) {
                    ConnectorExpression rem = remaining.isEmpty() ? Constant.TRUE
                            : remaining.size() == 1 ? remaining.get(0)
                            : new Call(call.getType(), call.getFunctionName(), remaining);
                    return new QueryStringExtraction(found, rem);
                }
            }
        }
        return new QueryStringExtraction(Optional.empty(), expression);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit)
    {
        OpenSearchTableHandle table = (OpenSearchTableHandle) handle;
        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }
        OpenSearchTableHandle newTable = new OpenSearchTableHandle(
                table.getPattern(), table.getResolvedIndices(), table.getConstraint(), OptionalLong.of(limit));
        return Optional.of(new LimitApplicationResult<>(newTable, false, false));
    }

    /**
     * Get columns for multiple indices, merging their schemas.
     * Same column name → keep type from first index.
     */
    private List<OpenSearchColumnHandle> getColumnsForIndices(List<String> indices)
    {
        LinkedHashMap<String, OpenSearchColumnHandle> merged = new LinkedHashMap<>();
        for (String idx : indices) {
            for (OpenSearchColumnHandle col : getColumnsForSingleIndex(idx)) {
                merged.putIfAbsent(col.getName(), col);
            }
        }
        return ImmutableList.copyOf(merged.values());
    }

    @SuppressWarnings("unchecked")
    private List<OpenSearchColumnHandle> getColumnsForSingleIndex(String indexName)
    {
        ClusterState state = clusterService.state();
        IndexMetadata indexMeta = state.metadata().index(indexName);
        if (indexMeta == null) {
            return ImmutableList.of();
        }
        MappingMetadata mapping = indexMeta.mapping();
        if (mapping == null) {
            return ImmutableList.of();
        }
        Map<String, Object> source = mapping.sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) source.get("properties");
        Map<String, Object> meta = (Map<String, Object>) source.get("_meta");
        // OpenSearch stores dynamic mappings in alphabetical order via sourceAsMap().
        // The legacy Calcite code path iterated fields through a HashMap in
        // OpenSearchDescribeIndexRequest.getFieldTypes(), producing a deterministic but
        // hash-based column order. Existing integration tests (CalcitePPLBasicIT,
        // PPLBankIT, etc.) rely on that ordering — their data-row matchers are
        // positional within each row. Re-wrapping properties in a HashMap here reproduces
        // that iteration order so we match the same behavior.
        Map<String, Object> hashOrdered = properties == null ? null : new HashMap<>(properties);
        return OpenSearchTypeMapper.mapColumns(hashOrdered, meta);
    }
}
