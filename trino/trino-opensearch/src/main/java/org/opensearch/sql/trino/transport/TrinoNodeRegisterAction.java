/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import org.opensearch.action.ActionType;

/** Action type for registering a node's Trino HTTP URL with remote nodes. */
public class TrinoNodeRegisterAction extends ActionType<TrinoNodeRegisterResponse> {

  public static final String NAME = "cluster:internal/trino/node/register";
  public static final TrinoNodeRegisterAction INSTANCE = new TrinoNodeRegisterAction();

  private TrinoNodeRegisterAction() {
    super(NAME, TrinoNodeRegisterResponse::new);
  }
}
