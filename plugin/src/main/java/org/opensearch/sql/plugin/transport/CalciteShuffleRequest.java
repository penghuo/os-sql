/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import java.util.List;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request to deliver shuffled data to a target partition on a node.
 *
 * <p>Contains the partition identifier, serialized row data (as a byte array for efficient
 * transport), column metadata, and the serialized plan fragment to execute after receiving the
 * shuffled data.
 */
@Getter
public class CalciteShuffleRequest extends ActionRequest {
    private final int partitionId;
    private final byte[] serializedRows;
    private final List<String> columnNames;
    private final String serializedPlan;

    public CalciteShuffleRequest(
            int partitionId,
            byte[] serializedRows,
            List<String> columnNames,
            String serializedPlan) {
        this.partitionId = partitionId;
        this.serializedRows = serializedRows;
        this.columnNames = columnNames;
        this.serializedPlan = serializedPlan;
    }

    public CalciteShuffleRequest(StreamInput in) throws IOException {
        super(in);
        this.partitionId = in.readVInt();
        this.serializedRows = in.readByteArray();
        this.columnNames = in.readStringList();
        this.serializedPlan = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(partitionId);
        out.writeByteArray(serializedRows);
        out.writeStringCollection(columnNames);
        out.writeString(serializedPlan);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException errors = null;
        if (serializedRows == null) {
            errors = new ActionRequestValidationException();
            errors.addValidationError("serializedRows is required");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            if (errors == null) {
                errors = new ActionRequestValidationException();
            }
            errors.addValidationError("columnNames is required");
        }
        if (serializedPlan == null || serializedPlan.isEmpty()) {
            if (errors == null) {
                errors = new ActionRequestValidationException();
            }
            errors.addValidationError("serializedPlan is required");
        }
        if (partitionId < 0) {
            if (errors == null) {
                errors = new ActionRequestValidationException();
            }
            errors.addValidationError("partitionId must be non-negative");
        }
        return errors;
    }
}
