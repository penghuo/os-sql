/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import java.io.IOException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Transport response for shard execution results. */
@Getter
@RequiredArgsConstructor
public class ShardExecutionResponse extends ActionResponse {

    /** Status of the shard execution. */
    public enum Status {
        SUCCESS,
        FAILED,
        CANCELLED
    }

    private final String queryId;
    private final int fragmentId;
    private final Status status;
    private final byte[] resultRows;
    private final boolean hasMore;
    private final long rowsProcessed;
    private final long executionTimeMs;
    private final String errorMessage;
    private final String executingNodeId;

    /** Deserialization constructor. */
    public ShardExecutionResponse(StreamInput in) throws IOException {
        super(in);
        queryId = in.readString();
        fragmentId = in.readVInt();
        status = in.readEnum(Status.class);
        resultRows = in.readByteArray();
        hasMore = in.readBoolean();
        rowsProcessed = in.readVLong();
        executionTimeMs = in.readVLong();
        errorMessage = in.readOptionalString();
        executingNodeId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(queryId);
        out.writeVInt(fragmentId);
        out.writeEnum(status);
        out.writeByteArray(resultRows);
        out.writeBoolean(hasMore);
        out.writeVLong(rowsProcessed);
        out.writeVLong(executionTimeMs);
        out.writeOptionalString(errorMessage);
        out.writeString(executingNodeId);
    }
}
