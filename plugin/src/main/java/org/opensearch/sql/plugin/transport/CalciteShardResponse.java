/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Response from shard-local Calcite plan execution.
 *
 * <p>Contains the result rows as serialized Object arrays, row type metadata for proper
 * deserialization on the coordinator, error information if execution failed, and optional binary
 * fields for carrying serialized partial aggregate states (HLL sketches, t-digest centroids,
 * Welford accumulators) during shuffle.
 */
@Getter
public class CalciteShardResponse extends ActionResponse {
    private final List<Object[]> rows;
    private final List<String> columnNames;
    private final int shardId;
    private final String errorMessage;
    private final Map<String, byte[]> binaryFields;

    /** Successful response with result rows. */
    public CalciteShardResponse(List<Object[]> rows, List<String> columnNames, int shardId) {
        this.rows = rows;
        this.columnNames = columnNames;
        this.shardId = shardId;
        this.errorMessage = null;
        this.binaryFields = null;
    }

    /** Successful response with result rows and binary partial aggregate states. */
    public CalciteShardResponse(
            List<Object[]> rows,
            List<String> columnNames,
            int shardId,
            Map<String, byte[]> binaryFields) {
        this.rows = rows;
        this.columnNames = columnNames;
        this.shardId = shardId;
        this.errorMessage = null;
        this.binaryFields = binaryFields;
    }

    /** Error response. */
    public CalciteShardResponse(int shardId, String errorMessage) {
        this.rows = List.of();
        this.columnNames = List.of();
        this.shardId = shardId;
        this.errorMessage = errorMessage;
        this.binaryFields = null;
    }

    public CalciteShardResponse(StreamInput in) throws IOException {
        super(in);
        this.shardId = in.readVInt();
        this.errorMessage = in.readOptionalString();
        this.columnNames = in.readStringList();
        int rowCount = in.readVInt();
        this.rows = new ArrayList<>(rowCount);
        int colCount = columnNames.size();
        for (int i = 0; i < rowCount; i++) {
            Object[] row = new Object[colCount];
            for (int j = 0; j < colCount; j++) {
                row[j] = in.readGenericValue();
            }
            rows.add(row);
        }
        // Read optional binary fields
        if (in.readBoolean()) {
            int fieldCount = in.readVInt();
            this.binaryFields = new HashMap<>(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                String key = in.readString();
                byte[] value = in.readByteArray();
                binaryFields.put(key, value);
            }
        } else {
            this.binaryFields = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeOptionalString(errorMessage);
        out.writeStringCollection(columnNames);
        out.writeVInt(rows.size());
        for (Object[] row : rows) {
            for (Object value : row) {
                out.writeGenericValue(value);
            }
        }
        // Write optional binary fields
        if (binaryFields != null && !binaryFields.isEmpty()) {
            out.writeBoolean(true);
            out.writeVInt(binaryFields.size());
            for (Map.Entry<String, byte[]> entry : binaryFields.entrySet()) {
                out.writeString(entry.getKey());
                out.writeByteArray(entry.getValue());
            }
        } else {
            out.writeBoolean(false);
        }
    }

    public boolean isError() {
        return errorMessage != null;
    }
}
