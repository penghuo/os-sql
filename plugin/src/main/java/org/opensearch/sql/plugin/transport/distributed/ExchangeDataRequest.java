/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import java.io.IOException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Transport request for sending exchange data between plan fragments. */
@Getter
@RequiredArgsConstructor
public class ExchangeDataRequest extends ActionRequest {
    private final String queryId;
    private final int sourceFragmentId;
    private final int targetFragmentId;
    private final int partitionId;
    private final byte[] rows;
    private final boolean isLastBatch;
    private final long sequenceNumber;

    /** Deserialization constructor. */
    public ExchangeDataRequest(StreamInput in) throws IOException {
        super(in);
        queryId = in.readString();
        sourceFragmentId = in.readVInt();
        targetFragmentId = in.readVInt();
        partitionId = in.readVInt();
        rows = in.readByteArray();
        isLastBatch = in.readBoolean();
        sequenceNumber = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        out.writeVInt(sourceFragmentId);
        out.writeVInt(targetFragmentId);
        out.writeVInt(partitionId);
        out.writeByteArray(rows);
        out.writeBoolean(isLastBatch);
        out.writeVLong(sequenceNumber);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
