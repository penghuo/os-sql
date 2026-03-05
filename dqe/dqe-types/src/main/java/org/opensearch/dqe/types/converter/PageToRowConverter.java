/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.converter;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.opensearch.dqe.types.DqeType;

/**
 * Converts Trino {@link Page}s (columnar format) to row-oriented {@code List<List<Object>>} for
 * inclusion in DQE query responses.
 *
 * <p>For each position (row) in the page, for each channel (column), the appropriate type accessor
 * is used to extract the value from the Block.
 */
public final class PageToRowConverter {

  private PageToRowConverter() {}

  /**
   * Convert a page to row data.
   *
   * @param page the Trino page to convert
   * @param outputTypes the DQE types for each output column
   * @return list of rows, where each row is a list of column values
   */
  public static List<List<Object>> convert(Page page, List<DqeType> outputTypes) {
    Objects.requireNonNull(page, "page must not be null");
    Objects.requireNonNull(outputTypes, "outputTypes must not be null");

    int rowCount = page.getPositionCount();
    int channelCount = page.getChannelCount();
    List<List<Object>> rows = new ArrayList<>(rowCount);

    for (int position = 0; position < rowCount; position++) {
      List<Object> row = new ArrayList<>(channelCount);
      for (int channel = 0; channel < channelCount; channel++) {
        Block block = page.getBlock(channel);
        if (block.isNull(position)) {
          row.add(null);
        } else {
          DqeType dqeType = outputTypes.get(channel);
          row.add(extractValue(block, position, dqeType));
        }
      }
      rows.add(row);
    }
    return rows;
  }

  private static Object extractValue(Block block, int position, DqeType dqeType) {
    Type trinoType = dqeType.getTrinoType();
    String baseName = trinoType.getBaseName();

    return switch (baseName) {
      case "varchar" -> trinoType.getSlice(block, position).toStringUtf8();
      case "bigint" -> trinoType.getLong(block, position);
      case "integer" -> (int) trinoType.getLong(block, position);
      case "smallint" -> (short) trinoType.getLong(block, position);
      case "tinyint" -> (byte) trinoType.getLong(block, position);
      case "double" -> trinoType.getDouble(block, position);
      case "real" -> Float.intBitsToFloat((int) trinoType.getLong(block, position));
      case "boolean" -> trinoType.getBoolean(block, position);
      case "timestamp" -> trinoType.getLong(block, position); // micros since epoch
      case "varbinary" -> trinoType.getSlice(block, position).getBytes();
      default -> trinoType.getObjectValue(null, block, position); // fallback
    };
  }
}
