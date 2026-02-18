/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request to execute a Calcite plan fragment on a specific shard.
 *
 * <p>Contains the serialized RelNode plan fragment (as JSON string), the target index name, and the
 * shard ordinal. The plan fragment will be deserialized and executed by ShardCalciteRuntime on the
 * data node.
 */
@Getter
public class CalciteShardRequest extends ActionRequest {
    private final String serializedPlan;
    private final String indexName;
    private final int shardId;

    public CalciteShardRequest(String serializedPlan, String indexName, int shardId) {
        this.serializedPlan = serializedPlan;
        this.indexName = indexName;
        this.shardId = shardId;
    }

    public CalciteShardRequest(StreamInput in) throws IOException {
        super(in);
        this.serializedPlan = in.readString();
        this.indexName = in.readString();
        this.shardId = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(serializedPlan);
        out.writeString(indexName);
        out.writeVInt(shardId);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException errors = null;
        if (serializedPlan == null || serializedPlan.isEmpty()) {
            errors = new ActionRequestValidationException();
            errors.addValidationError("serializedPlan is required");
        }
        if (indexName == null || indexName.isEmpty()) {
            if (errors == null) {
                errors = new ActionRequestValidationException();
            }
            errors.addValidationError("indexName is required");
        }
        if (shardId < 0) {
            if (errors == null) {
                errors = new ActionRequestValidationException();
            }
            errors.addValidationError("shardId must be non-negative");
        }
        return errors;
    }
}
