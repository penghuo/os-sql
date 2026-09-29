/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

abstract class AbstractPPLAsyncQueryRequest extends ActionRequest {
  private final String id;

  AbstractPPLAsyncQueryRequest(String id) {
    this.id = id;
  }

  AbstractPPLAsyncQueryRequest(StreamInput in) throws IOException {
    super(in);
    id = in.readString();
  }

  String id() {
    return id;
  }

  /** {@inheritDoc} */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(id);
  }

  /** {@inheritDoc} */
  @Override
  public ActionRequestValidationException validate() {
    ActionRequestValidationException validationException = null;
    if (id == null || id.isBlank()) {
      validationException = addValidationError("[id] must not be empty", validationException);
    }
    return validationException;
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "PPL asynchronous query lifecycle request";
  }
}
