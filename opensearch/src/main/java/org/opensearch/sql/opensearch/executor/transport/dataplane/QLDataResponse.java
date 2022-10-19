/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.executor.transport.dataplane;

import static org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory.OBJECT_MAPPER;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.List;
import lombok.Getter;
import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.sql.data.model.ExprValue;

public class QLDataResponse extends ActionResponse {

  private boolean success;

  @Getter
  private List<ExprValue> values;

  public QLDataResponse(boolean success, List<ExprValue> values) {
    super();
    this.success = success;
    this.values = values;
  }

  public QLDataResponse(StreamInput in) throws IOException {
    this.success = in.readBoolean();
    this.values = OBJECT_MAPPER.readValue(in.readString(), new TypeReference<>() {});
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeBoolean(success);
    out.writeString(OBJECT_MAPPER.writeValueAsString(values));
  }
}
