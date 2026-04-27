/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public final class OpenSearchTableHandle
        implements ConnectorTableHandle
{
    // Original pattern or single index name (e.g., "logs-*" or "logs-2024,logs-2025")
    private final String pattern;
    // Resolved concrete indices (e.g., ["logs-2024-01", "logs-2024-02"])
    private final List<String> resolvedIndices;
    private final TupleDomain<OpenSearchColumnHandle> constraint;
    private final OptionalLong limit;
    // LIKE patterns pushed down: opensearchFieldName -> SQL LIKE pattern (e.g., "%google%")
    private final Map<String, String> likePatterns;
    // query_string expression pushed down to Lucene QueryStringQueryParser
    private final Optional<String> queryStringExpression;

    // Convenience constructor for single literal index (backward-compatible)
    public OpenSearchTableHandle(String index)
    {
        this(index, ImmutableList.of(index), TupleDomain.all(), OptionalLong.empty(), ImmutableMap.of(), Optional.empty());
    }

    // Constructor for multi-index resolution
    public OpenSearchTableHandle(String pattern, List<String> resolvedIndices)
    {
        this(pattern, resolvedIndices, TupleDomain.all(), OptionalLong.empty(), ImmutableMap.of(), Optional.empty());
    }

    // Constructor with constraint and limit (used in applyFilter/applyLimit)
    public OpenSearchTableHandle(String pattern, List<String> resolvedIndices, TupleDomain<OpenSearchColumnHandle> constraint, OptionalLong limit)
    {
        this(pattern, resolvedIndices, constraint, limit, ImmutableMap.of(), Optional.empty());
    }

    @JsonCreator
    public OpenSearchTableHandle(
            @JsonProperty("pattern") String pattern,
            @JsonProperty("resolvedIndices") List<String> resolvedIndices,
            @JsonProperty("constraint") TupleDomain<OpenSearchColumnHandle> constraint,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("likePatterns") Map<String, String> likePatterns,
            @JsonProperty("queryStringExpression") Optional<String> queryStringExpression)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.resolvedIndices = ImmutableList.copyOf(requireNonNull(resolvedIndices, "resolvedIndices is null"));
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.likePatterns = ImmutableMap.copyOf(requireNonNull(likePatterns, "likePatterns is null"));
        this.queryStringExpression = requireNonNull(queryStringExpression, "queryStringExpression is null");
    }

    @JsonProperty
    public String getPattern()
    {
        return pattern;
    }

    @JsonProperty
    public List<String> getResolvedIndices()
    {
        return resolvedIndices;
    }

    @JsonProperty
    public TupleDomain<OpenSearchColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Map<String, String> getLikePatterns()
    {
        return likePatterns;
    }

    @JsonProperty
    public Optional<String> getQueryStringExpression()
    {
        return queryStringExpression;
    }

    // Backward-compatible: return first resolved index or pattern
    public String getIndex()
    {
        return resolvedIndices.isEmpty() ? pattern : resolvedIndices.get(0);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof OpenSearchTableHandle other)) return false;
        return pattern.equals(other.pattern)
                && resolvedIndices.equals(other.resolvedIndices)
                && constraint.equals(other.constraint)
                && limit.equals(other.limit)
                && likePatterns.equals(other.likePatterns)
                && queryStringExpression.equals(other.queryStringExpression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(pattern, resolvedIndices, constraint, limit, likePatterns, queryStringExpression);
    }

    @Override
    public String toString()
    {
        if (resolvedIndices.size() == 1 && resolvedIndices.get(0).equals(pattern)) {
            return "opensearch:" + pattern;
        }
        return "opensearch:pattern=" + pattern + ";indices=" + resolvedIndices;
    }
}
