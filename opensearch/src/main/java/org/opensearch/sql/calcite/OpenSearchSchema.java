/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.sql.calcite;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.AliasMetadata;

/**
 * Each table in the schema is an OpenSearch index.
 */
public class OpenSearchSchema extends AbstractSchema {

  private final NodeClient client;

  private final ObjectMapper mapper;

  private final Map<String, Table> tableMap;

  /**
   * Default batch size to be used during scrolling.
   */
  private final int fetchSize;

  /**
   * Allows schema to be instantiated from existing elastic search client.
   *
   * @param client existing client instance
   * @param mapper mapper for JSON (de)serialization
   * @param index name of ES index
   */
  public OpenSearchSchema(NodeClient client, ObjectMapper mapper,
                          @Nullable String index) {
    this(client, mapper, index, OpenSearchTransport.DEFAULT_FETCH_SIZE);
  }

  @VisibleForTesting
  OpenSearchSchema(NodeClient client, ObjectMapper mapper,
                   @Nullable String index, int fetchSize) {
    super();
    this.client = requireNonNull(client, "client");
    this.mapper = requireNonNull(mapper, "mapper");
    checkArgument(fetchSize > 0,
        "invalid fetch size. Expected %s > 0", fetchSize);
    this.fetchSize = fetchSize;

    if (index == null) {
      try {
        this.tableMap = createTables(indicesFromOpenSearch());
      } catch (IOException e) {
        throw new UncheckedIOException("Couldn't get indices", e);
      }
    } else {
      this.tableMap = createTables(Collections.singleton(index));
    }
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  private Map<String, Table> createTables(Iterable<String> indices) {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (String index : indices) {
      final OpenSearchTransport transport =
          new OpenSearchTransport(client, mapper, index, fetchSize);
      builder.put(index, new OpenSearchTable(transport));
    }
    return builder.build();
  }

  /**
   * Queries {@code _alias} definition to automatically detect all indices.
   *
   * @return list of indices
   * @throws IOException for any IO related issues
   * @throws IllegalStateException if reply is not understood
   */
  private Set<String> indicesFromOpenSearch() throws IOException {
    final GetIndexResponse indexResponse =
        client.admin().indices().prepareGetIndex().setLocal(true).get();
    final Stream<String> aliasStream =
        ImmutableList.copyOf(indexResponse.aliases().values()).stream()
            .flatMap(Collection::stream)
            .map(AliasMetadata::alias);

    return Stream.concat(Arrays.stream(indexResponse.getIndices()), aliasStream)
        .collect(Collectors.toSet());
  }

}
