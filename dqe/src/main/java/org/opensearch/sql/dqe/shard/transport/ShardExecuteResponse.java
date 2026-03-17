/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import io.trino.spi.Page;
import io.trino.spi.type.Type;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.dqe.common.serde.PageSerializer;

/**
 * Transport response carrying the result of a shard-level DQE plan fragment execution. The result
 * is encoded as serialized Trino Pages with column type information, enabling the coordinator to
 * merge results without JSON parsing overhead.
 */
@Getter
public class ShardExecuteResponse extends ActionResponse {

  /** The result pages produced by the shard-level plan fragment execution. */
  private final List<Page> pages;

  /** The Trino types for each column in the result pages. */
  private final List<Type> columnTypes;

  /**
   * Optional per-group distinct value sets for COUNT(DISTINCT) optimization. Key: original GROUP BY
   * key value (e.g., RegionID as Long). Value: set of distinct values (e.g., UserIDs) for that
   * group. Only populated for local execution; null for remote shards.
   */
  @lombok.Setter
  private transient java.util.Map<Long, org.opensearch.sql.dqe.operator.LongOpenHashSet>
      distinctSets;

  /**
   * Optional per-group distinct value sets for VARCHAR-keyed COUNT(DISTINCT) optimization. Key:
   * GROUP BY varchar value (e.g., SearchPhrase). Value: set of distinct numeric values (e.g.,
   * UserIDs). Only populated for local execution; null for remote shards.
   */
  @lombok.Setter
  private transient java.util.Map<String, org.opensearch.sql.dqe.operator.LongOpenHashSet>
      varcharDistinctSets;

  public ShardExecuteResponse(List<Page> pages, List<Type> columnTypes) {
    this.pages = pages;
    this.columnTypes = columnTypes;
  }

  /** Deserialize from a stream. */
  public ShardExecuteResponse(StreamInput in) throws IOException {
    super(in);
    this.pages = PageSerializer.readPages(in);
    int typeCount = in.readVInt();
    this.columnTypes = new ArrayList<>(typeCount);
    for (int i = 0; i < typeCount; i++) {
      columnTypes.add(PageSerializer.resolveType(in.readString()));
    }
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    PageSerializer.writePages(out, pages, columnTypes);
    out.writeVInt(columnTypes.size());
    for (Type type : columnTypes) {
      out.writeString(type.getTypeSignature().toString());
    }
  }
}
