/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import java.io.IOException;
import org.opensearch.core.common.io.stream.StreamInput;

/** Request to cancel and remove a retained asynchronous PPL query. */
public final class PPLAsyncDeleteRequest extends AbstractPPLAsyncQueryRequest {
  /**
   * Creates a DELETE request.
   *
   * @param id opaque asynchronous query ID
   */
  public PPLAsyncDeleteRequest(String id) {
    super(id);
  }

  /**
   * Reads a DELETE request from the transport stream.
   *
   * @param in transport stream
   * @throws IOException when the request cannot be read
   */
  public PPLAsyncDeleteRequest(StreamInput in) throws IOException {
    super(in);
  }
}
