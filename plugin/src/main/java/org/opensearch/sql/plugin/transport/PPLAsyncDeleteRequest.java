/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import org.opensearch.core.common.io.stream.StreamInput;

public final class PPLAsyncDeleteRequest extends AbstractPPLAsyncQueryRequest {
  public PPLAsyncDeleteRequest(String id) {
    super(id);
  }

  public PPLAsyncDeleteRequest(StreamInput in) throws IOException {
    super(in);
  }
}
