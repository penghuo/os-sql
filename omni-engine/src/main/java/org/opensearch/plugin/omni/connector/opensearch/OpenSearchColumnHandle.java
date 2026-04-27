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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class OpenSearchColumnHandle
        implements ColumnHandle
{
    // name: lowercase, used by Trino SQL resolution
    private final String name;
    // opensearchName: original case from OpenSearch mapping, used for doc_values/_source field access
    private final String opensearchName;
    private final Type type;
    private final boolean supportsPredicates;

    @JsonCreator
    public OpenSearchColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("opensearchName") String opensearchName,
            @JsonProperty("type") Type type,
            @JsonProperty("supportsPredicates") boolean supportsPredicates)
    {
        this.name = requireNonNull(name, "name is null");
        this.opensearchName = requireNonNull(opensearchName, "opensearchName is null");
        this.type = requireNonNull(type, "type is null");
        this.supportsPredicates = supportsPredicates;
    }

    /** Convenience: when name == opensearchName (already lowercase or built-in) */
    public OpenSearchColumnHandle(String name, Type type, boolean supportsPredicates)
    {
        this(name, name, type, supportsPredicates);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    /** Original field name in OpenSearch (preserves case for doc_values/_source access) */
    @JsonProperty
    public String getOpensearchName()
    {
        return opensearchName;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean isSupportsPredicates()
    {
        return supportsPredicates;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof OpenSearchColumnHandle other)) return false;
        return name.equals(other.name) && type.equals(other.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public String toString()
    {
        return name + ":" + type;
    }
}
