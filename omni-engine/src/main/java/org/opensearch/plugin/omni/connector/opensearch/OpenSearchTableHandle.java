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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public final class OpenSearchTableHandle
        implements ConnectorTableHandle
{
    private final String index;
    private final TupleDomain<OpenSearchColumnHandle> constraint;
    private final OptionalLong limit;
    // LIKE patterns pushed down: opensearchFieldName -> SQL LIKE pattern (e.g., "%google%")
    private final Map<String, String> likePatterns;
    // query_string expression pushed down to Lucene QueryStringQueryParser
    private final Optional<String> queryStringExpression;

    public OpenSearchTableHandle(String index)
    {
        this(index, TupleDomain.all(), OptionalLong.empty(), ImmutableMap.of(), Optional.empty());
    }

    public OpenSearchTableHandle(String index, TupleDomain<OpenSearchColumnHandle> constraint, OptionalLong limit)
    {
        this(index, constraint, limit, ImmutableMap.of(), Optional.empty());
    }

    @JsonCreator
    public OpenSearchTableHandle(
            @JsonProperty("index") String index,
            @JsonProperty("constraint") TupleDomain<OpenSearchColumnHandle> constraint,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("likePatterns") Map<String, String> likePatterns,
            @JsonProperty("queryStringExpression") Optional<String> queryStringExpression)
    {
        this.index = requireNonNull(index, "index is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.likePatterns = ImmutableMap.copyOf(requireNonNull(likePatterns, "likePatterns is null"));
        this.queryStringExpression = requireNonNull(queryStringExpression, "queryStringExpression is null");
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof OpenSearchTableHandle other)) return false;
        return index.equals(other.index)
                && constraint.equals(other.constraint)
                && limit.equals(other.limit)
                && likePatterns.equals(other.likePatterns)
                && queryStringExpression.equals(other.queryStringExpression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(index, constraint, limit, likePatterns, queryStringExpression);
    }

    @Override
    public String toString()
    {
        return "opensearch:" + index;
    }
}
