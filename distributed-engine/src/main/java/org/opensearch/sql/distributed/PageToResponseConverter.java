/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BooleanArrayBlock;
import org.opensearch.sql.distributed.data.ByteArrayBlock;
import org.opensearch.sql.distributed.data.DictionaryBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.RunLengthEncodedBlock;
import org.opensearch.sql.distributed.data.ShortArrayBlock;
import org.opensearch.sql.distributed.data.ValueBlock;
import org.opensearch.sql.distributed.data.VariableWidthBlock;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

/**
 * Converts distributed engine output Pages to the {@link ExecutionEngine.QueryResponse} format
 * expected by the protocol layer. Extracts column values from typed Blocks and wraps them as
 * ExprValues matching the schema derived from the Calcite RelDataType.
 */
public final class PageToResponseConverter {

  private PageToResponseConverter() {}

  /**
   * Converts a list of output Pages and a Calcite row type into a QueryResponse.
   *
   * @param pages the output pages from the distributed pipeline
   * @param rowType the Calcite row type describing the output schema
   * @return a QueryResponse with schema and data rows
   */
  public static ExecutionEngine.QueryResponse convert(List<Page> pages, RelDataType rowType) {
    Schema schema = buildSchema(rowType);
    List<ExprValue> rows = extractRows(pages, rowType);
    ExecutionEngine.QueryResponse response = new ExecutionEngine.QueryResponse(schema, rows, null);
    response.setEngine(ExecutionEngine.ENGINE_DISTRIBUTED);
    return response;
  }

  private static Schema buildSchema(RelDataType rowType) {
    List<Column> columns = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      ExprType exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(field.getType());
      columns.add(new Column(field.getName(), null, exprType));
    }
    return new Schema(columns);
  }

  private static List<ExprValue> extractRows(List<Page> pages, RelDataType rowType) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    List<ExprValue> rows = new ArrayList<>();
    for (Page page : pages) {
      int positionCount = page.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        Map<String, ExprValue> row = new LinkedHashMap<>();
        for (int ch = 0; ch < page.getChannelCount(); ch++) {
          Block block = page.getBlock(ch);
          String columnName = ch < fields.size() ? fields.get(ch).getName() : "col" + ch;
          boolean isTimeBased =
              ch < fields.size() && OpenSearchTypeFactory.isTimeBasedType(fields.get(ch).getType());
          row.put(columnName, extractValue(block, pos, isTimeBased));
        }
        rows.add(ExprTupleValue.fromExprValueMap(row));
      }
    }
    return rows;
  }

  /**
   * Extracts a single value from a Block at the given position, handling all Block types including
   * DictionaryBlock and RunLengthEncodedBlock wrappers.
   */
  static ExprValue extractValue(Block block, int position) {
    return extractValue(block, position, false);
  }

  /**
   * Extracts a single value from a Block at the given position, with optional flag for time-based
   * conversion (epoch millis to timestamp).
   */
  static ExprValue extractValue(Block block, int position, boolean isTimeBased) {
    if (block.isNull(position)) {
      return ExprValueUtils.nullValue();
    }
    if (block instanceof ValueBlock valueBlock) {
      return extractFromValueBlock(valueBlock, position, isTimeBased);
    } else if (block instanceof DictionaryBlock dict) {
      return extractFromValueBlock(dict.getDictionary(), dict.getId(position), isTimeBased);
    } else if (block instanceof RunLengthEncodedBlock rle) {
      Block inner = rle.getValue();
      if (inner instanceof ValueBlock vb) {
        return extractFromValueBlock(vb, 0, isTimeBased);
      }
      return extractValue(inner, 0, isTimeBased);
    }
    throw new UnsupportedOperationException(
        "Unsupported Block type: " + block.getClass().getSimpleName());
  }

  private static ExprValue extractFromValueBlock(
      ValueBlock block, int position, boolean isTimeBased) {
    if (block.isNull(position)) {
      return ExprValueUtils.nullValue();
    }
    if (block instanceof LongArrayBlock longBlock) {
      long value = longBlock.getLong(position);
      // Convert epoch millis to timestamp for date/timestamp columns
      if (isTimeBased) {
        return ExprValueUtils.timestampValue(Instant.ofEpochMilli(value));
      }
      return ExprValueUtils.longValue(value);
    } else if (block instanceof IntArrayBlock intBlock) {
      return ExprValueUtils.integerValue(intBlock.getInt(position));
    } else if (block instanceof DoubleArrayBlock doubleBlock) {
      return ExprValueUtils.doubleValue(doubleBlock.getDouble(position));
    } else if (block instanceof BooleanArrayBlock boolBlock) {
      return ExprValueUtils.booleanValue(boolBlock.getBoolean(position));
    } else if (block instanceof ShortArrayBlock shortBlock) {
      return ExprValueUtils.shortValue(shortBlock.getShort(position));
    } else if (block instanceof ByteArrayBlock byteBlock) {
      return ExprValueUtils.byteValue(byteBlock.getByte(position));
    } else if (block instanceof VariableWidthBlock varBlock) {
      return ExprValueUtils.stringValue(
          new String(varBlock.getSlice(position), StandardCharsets.UTF_8));
    }
    throw new UnsupportedOperationException(
        "Unsupported ValueBlock type: " + block.getClass().getSimpleName());
  }
}
