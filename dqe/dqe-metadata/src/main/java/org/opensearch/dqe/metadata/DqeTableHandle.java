/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * Immutable handle representing a resolved OpenSearch index/table. Carries the index name, optional
 * wildcard pattern, resolved concrete indices, schema generation for snapshot consistency, and
 * optional PIT ID for read consistency.
 *
 * <p>Implements {@link Writeable} for transport serialization between coordinator and data nodes.
 */
public final class DqeTableHandle implements Writeable {

  private final String indexName;
  @Nullable private final String indexPattern;
  private final List<String> resolvedIndices;
  private final long schemaGeneration;
  @Nullable private final String pitId;

  public DqeTableHandle(
      String indexName,
      @Nullable String indexPattern,
      List<String> resolvedIndices,
      long schemaGeneration,
      @Nullable String pitId) {
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.indexPattern = indexPattern;
    this.resolvedIndices =
        resolvedIndices != null ? List.copyOf(resolvedIndices) : List.of(indexName);
    this.schemaGeneration = schemaGeneration;
    this.pitId = pitId;
  }

  /** Deserializes from a stream. */
  public DqeTableHandle(StreamInput in) throws IOException {
    this.indexName = in.readString();
    this.indexPattern = in.readOptionalString();
    this.resolvedIndices = in.readStringList();
    this.schemaGeneration = in.readLong();
    this.pitId = in.readOptionalString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(indexName);
    out.writeOptionalString(indexPattern);
    out.writeStringCollection(resolvedIndices);
    out.writeLong(schemaGeneration);
    out.writeOptionalString(pitId);
  }

  public String getIndexName() {
    return indexName;
  }

  @Nullable
  public String getIndexPattern() {
    return indexPattern;
  }

  public List<String> getResolvedIndices() {
    return resolvedIndices;
  }

  public long getSchemaGeneration() {
    return schemaGeneration;
  }

  @Nullable
  public String getPitId() {
    return pitId;
  }

  public DqeTableHandle withPitId(String pitId) {
    return new DqeTableHandle(indexName, indexPattern, resolvedIndices, schemaGeneration, pitId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DqeTableHandle other)) {
      return false;
    }
    return schemaGeneration == other.schemaGeneration && indexName.equals(other.indexName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexName, schemaGeneration);
  }

  @Override
  public String toString() {
    return "DqeTableHandle{"
        + indexName
        + (indexPattern != null ? ", pattern=" + indexPattern : "")
        + ", gen="
        + schemaGeneration
        + (pitId != null ? ", pit=true" : "")
        + "}";
  }
}
