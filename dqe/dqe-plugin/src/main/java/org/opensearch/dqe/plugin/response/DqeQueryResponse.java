/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.response;

import java.util.Collections;
import java.util.List;

/**
 * Immutable DQE query execution response. Contains the engine identifier, result schema, data rows,
 * and execution statistics per design doc Section 6.4.
 */
public class DqeQueryResponse {

  private final String engine;
  private final List<ColumnSchema> schema;
  private final List<List<Object>> data;
  private final QueryStats stats;

  private DqeQueryResponse(Builder builder) {
    this.engine = builder.engine;
    this.schema = builder.schema == null ? Collections.emptyList() : List.copyOf(builder.schema);
    this.data = builder.data == null ? Collections.emptyList() : List.copyOf(builder.data);
    this.stats = builder.stats;
  }

  public String getEngine() {
    return engine;
  }

  public List<ColumnSchema> getSchema() {
    return schema;
  }

  public List<List<Object>> getData() {
    return data;
  }

  public QueryStats getStats() {
    return stats;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Column metadata in the result schema. */
  public static class ColumnSchema {
    private final String name;
    private final String type;

    public ColumnSchema(String name, String type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }
  }

  /** Execution statistics for a completed query. */
  public static class QueryStats {
    private final String state;
    private final String queryId;
    private final long elapsedMs;
    private final long rowsProcessed;
    private final long bytesProcessed;
    private final int stages;
    private final int shardsQueried;

    private QueryStats(Builder builder) {
      this.state = builder.state;
      this.queryId = builder.queryId;
      this.elapsedMs = builder.elapsedMs;
      this.rowsProcessed = builder.rowsProcessed;
      this.bytesProcessed = builder.bytesProcessed;
      this.stages = builder.stages;
      this.shardsQueried = builder.shardsQueried;
    }

    public String getState() {
      return state;
    }

    public String getQueryId() {
      return queryId;
    }

    public long getElapsedMs() {
      return elapsedMs;
    }

    public long getRowsProcessed() {
      return rowsProcessed;
    }

    public long getBytesProcessed() {
      return bytesProcessed;
    }

    public int getStages() {
      return stages;
    }

    public int getShardsQueried() {
      return shardsQueried;
    }

    public static Builder builder() {
      return new Builder();
    }

    /** Builder for QueryStats. */
    public static class Builder {
      private String state = "COMPLETED";
      private String queryId;
      private long elapsedMs;
      private long rowsProcessed;
      private long bytesProcessed;
      private int stages;
      private int shardsQueried;

      public Builder state(String state) {
        this.state = state;
        return this;
      }

      public Builder queryId(String queryId) {
        this.queryId = queryId;
        return this;
      }

      public Builder elapsedMs(long elapsedMs) {
        this.elapsedMs = elapsedMs;
        return this;
      }

      public Builder rowsProcessed(long rowsProcessed) {
        this.rowsProcessed = rowsProcessed;
        return this;
      }

      public Builder bytesProcessed(long bytesProcessed) {
        this.bytesProcessed = bytesProcessed;
        return this;
      }

      public Builder stages(int stages) {
        this.stages = stages;
        return this;
      }

      public Builder shardsQueried(int shardsQueried) {
        this.shardsQueried = shardsQueried;
        return this;
      }

      public QueryStats build() {
        return new QueryStats(this);
      }
    }
  }

  /** Builder for DqeQueryResponse. */
  public static class Builder {
    private String engine = "dqe";
    private List<ColumnSchema> schema;
    private List<List<Object>> data;
    private QueryStats stats;

    public Builder engine(String engine) {
      this.engine = engine;
      return this;
    }

    public Builder schema(List<ColumnSchema> schema) {
      this.schema = schema;
      return this;
    }

    public Builder data(List<List<Object>> data) {
      this.data = data;
      return this;
    }

    public Builder stats(QueryStats stats) {
      this.stats = stats;
      return this;
    }

    public DqeQueryResponse build() {
      return new DqeQueryResponse(this);
    }
  }
}
