/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3.transport;

import org.opensearch.action.ActionType;

public class CreateViewAction extends ActionType<CreateViewResponse> {
  public static final CreateViewAction INSTANCE = new CreateViewAction();
  public static final String NAME = "cluster:opensearch/view/create";

  private CreateViewAction() {
    super(NAME, CreateViewResponse::new);
  }
}
