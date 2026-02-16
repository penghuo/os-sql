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

/** Transport request for cancelling a distributed query. */
@Getter
@RequiredArgsConstructor
public class QueryCancelRequest extends ActionRequest {
    private final String queryId;
    private final String reason;

    /** Deserialization constructor. */
    public QueryCancelRequest(StreamInput in) throws IOException {
        super(in);
        queryId = in.readString();
        reason = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        out.writeOptionalString(reason);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
