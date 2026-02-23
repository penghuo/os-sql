/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import org.opensearch.action.ActionType;

/**
 * Transport action for cancelling a running stage on a data node.
 *
 * <p>Action name: {@code internal:dqe/stage/cancel}
 */
public class DqeStageCancelAction extends ActionType<DqeStageCancelResponse> {

  public static final String NAME = "internal:dqe/stage/cancel";
  public static final DqeStageCancelAction INSTANCE = new DqeStageCancelAction();

  private DqeStageCancelAction() {
    super(NAME, DqeStageCancelResponse::new);
  }
}
