/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.storage.fs;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.streaming.StreamingSource;
import org.opensearch.sql.filesystem.streaming.FileSystemStreamSource;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

@Getter
@RequiredArgsConstructor
public class FSTable implements Table {

  private final FileSystem fs;

  private final CompressionCodec compressionCodec;

  private final Path basePath;

  private final Map<String, String> fieldType;

  private static final Map<String, ExprType> S3_TYPE_TO_EXPR_TYPE_MAPPING =
      ImmutableMap.<String, ExprType>builder()
          .put("string", ExprCoreType.STRING)
          .put("byte", ExprCoreType.BYTE)
          .put("short", ExprCoreType.SHORT)
          .put("integer", ExprCoreType.INTEGER)
          .put("long", ExprCoreType.LONG)
          .put("float", ExprCoreType.FLOAT)
          .put("half_float", ExprCoreType.FLOAT)
          .put("scaled_float", ExprCoreType.DOUBLE)
          .put("double", ExprCoreType.DOUBLE)
          .put("boolean", ExprCoreType.BOOLEAN)
          .put("nested", ExprCoreType.ARRAY)
          .put("object", ExprCoreType.STRUCT)
          .put("date", ExprCoreType.TIMESTAMP)
          .put("date_nanos", ExprCoreType.TIMESTAMP)
          .build();

  @Override
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    for (Map.Entry<String, String> entry : fieldType.entrySet()) {
      fieldTypes.put(
          entry.getKey(), S3_TYPE_TO_EXPR_TYPE_MAPPING.getOrDefault(entry.getValue(), UNKNOWN));
    }
    return fieldTypes;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new S3PlanImplementor(), null);
  }

  @Override
  public StreamingSource asStreamingSource() {
    return new FileSystemStreamSource(fs, basePath);
  }

  @Override
  public String toString() {
    return StringUtils.format("FSTable %s, basePath = %s", fs.getScheme(), basePath);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public class S3PlanImplementor extends DefaultImplementor<Void> {
    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Void context) {
      return new FSScanOperator(fs, basePath, compressionCodec, getFieldTypes());
    }
  }
}
