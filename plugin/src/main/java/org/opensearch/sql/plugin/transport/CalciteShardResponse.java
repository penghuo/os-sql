/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Transport response containing typed result rows from a shard-level Calcite plan execution. */
@Getter
public class CalciteShardResponse extends ActionResponse {

  private final List<Object[]> rows;
  private final List<String> columnNames;
  private final List<SqlTypeName> columnTypes;
  private final Exception error;

  /** Construct a successful response. */
  public CalciteShardResponse(
      List<Object[]> rows, List<String> columnNames, List<SqlTypeName> columnTypes) {
    this.rows = rows;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.error = null;
  }

  /** Construct an error response. */
  public CalciteShardResponse(Exception error) {
    this.rows = Collections.emptyList();
    this.columnNames = Collections.emptyList();
    this.columnTypes = Collections.emptyList();
    this.error = error;
  }

  /** Deserialize from stream. */
  public CalciteShardResponse(StreamInput in) throws IOException {
    super(in);
    boolean hasError = in.readBoolean();
    if (hasError) {
      String errorMessage = in.readString();
      this.error = new RuntimeException(errorMessage);
      this.rows = Collections.emptyList();
      this.columnNames = Collections.emptyList();
      this.columnTypes = Collections.emptyList();
    } else {
      this.error = null;
      int numColumns = in.readVInt();
      List<String> names = new ArrayList<>(numColumns);
      List<SqlTypeName> types = new ArrayList<>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        names.add(in.readString());
        types.add(SqlTypeName.valueOf(in.readString()));
      }
      this.columnNames = names;
      this.columnTypes = types;

      int numRows = in.readVInt();
      List<Object[]> rowList = new ArrayList<>(numRows);
      for (int r = 0; r < numRows; r++) {
        int cols = in.readVInt();
        Object[] row = new Object[cols];
        for (int c = 0; c < cols; c++) {
          row[c] = in.readGenericValue();
        }
        rowList.add(row);
      }
      this.rows = rowList;
    }
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    boolean hasError = error != null;
    out.writeBoolean(hasError);
    if (hasError) {
      out.writeString(error.getMessage() != null ? error.getMessage() : "Unknown error");
    } else {
      int numColumns = columnNames.size();
      out.writeVInt(numColumns);
      for (int i = 0; i < numColumns; i++) {
        out.writeString(columnNames.get(i));
        out.writeString(columnTypes.get(i).name());
      }

      out.writeVInt(rows.size());
      for (Object[] row : rows) {
        out.writeVInt(row.length);
        for (Object value : row) {
          out.writeGenericValue(value);
        }
      }
    }
  }
}
