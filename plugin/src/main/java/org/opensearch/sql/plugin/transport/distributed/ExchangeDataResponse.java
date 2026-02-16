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

/** Transport response for exchange data acknowledgement. */
@Getter
@RequiredArgsConstructor
public class ExchangeDataResponse extends ActionResponse {
    private final boolean accepted;
    private final long availableBufferBytes;
    private final long acknowledgedSequence;

    /** Deserialization constructor. */
    public ExchangeDataResponse(StreamInput in) throws IOException {
        super(in);
        accepted = in.readBoolean();
        availableBufferBytes = in.readVLong();
        acknowledgedSequence = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(accepted);
        out.writeVLong(availableBufferBytes);
        out.writeVLong(acknowledgedSequence);
    }
}
