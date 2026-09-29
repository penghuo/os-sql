/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Request to read a retained asynchronous PPL query result and optionally renew its lease. */
public final class PPLAsyncGetResultRequest extends AbstractPPLAsyncQueryRequest {
  private final TimeValue keepAlive;

  /**
   * Creates a GET result request.
   *
   * @param id opaque asynchronous query ID
   * @param keepAlive optional replacement lease duration
   */
  public PPLAsyncGetResultRequest(String id, TimeValue keepAlive) {
    super(id);
    this.keepAlive = keepAlive;
  }

  /**
   * Reads a GET result request from the transport stream.
   *
   * @param in transport stream
   * @throws IOException when the request cannot be read
   */
  public PPLAsyncGetResultRequest(StreamInput in) throws IOException {
    super(in);
    keepAlive = in.readOptionalTimeValue();
  }

  TimeValue keepAlive() {
    return keepAlive;
  }

  /** {@inheritDoc} */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeOptionalTimeValue(keepAlive);
  }

  /** {@inheritDoc} */
  @Override
  public ActionRequestValidationException validate() {
    ActionRequestValidationException validationException = super.validate();
    if (keepAlive != null && keepAlive.millis() <= 0) {
      validationException =
          addValidationError("[keep_alive] must be greater than 0", validationException);
    }
    return validationException;
  }
}
