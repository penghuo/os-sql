/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import java.io.IOException;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Transport request for executing a plan fragment on a specific shard. */
@Getter
@RequiredArgsConstructor
public class ShardExecutionRequest extends ActionRequest {
    private final String queryId;
    private final int fragmentId;
    private final byte[] serializedPlan;
    private final String indexName;
    private final int shardId;
    private final Map<String, String> settings;

    /** Deserialization constructor. */
    public ShardExecutionRequest(StreamInput in) throws IOException {
        super(in);
        queryId = in.readString();
        fragmentId = in.readVInt();
        serializedPlan = in.readByteArray();
        indexName = in.readString();
        shardId = in.readVInt();
        settings = in.readMap(StreamInput::readString, StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        out.writeVInt(fragmentId);
        out.writeByteArray(serializedPlan);
        out.writeString(indexName);
        out.writeVInt(shardId);
        out.writeMap(settings, StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
