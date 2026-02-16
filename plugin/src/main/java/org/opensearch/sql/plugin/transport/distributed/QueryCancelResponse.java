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

/** Transport response for query cancellation. */
@Getter
@RequiredArgsConstructor
public class QueryCancelResponse extends ActionResponse {
    private final boolean acknowledged;
    private final int fragmentsCancelled;

    /** Deserialization constructor. */
    public QueryCancelResponse(StreamInput in) throws IOException {
        super(in);
        acknowledged = in.readBoolean();
        fragmentsCancelled = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
        out.writeVInt(fragmentsCancelled);
    }
}
