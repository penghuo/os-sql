/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import org.opensearch.action.ActionType;

/**
 * Transport action for either side signaling a failure/abort in the exchange.
 *
 * <p>Action name: {@code internal:dqe/exchange/abort}
 */
public class DqeExchangeAbortAction extends ActionType<DqeExchangeAbortResponse> {

  public static final String NAME = "internal:dqe/exchange/abort";
  public static final DqeExchangeAbortAction INSTANCE = new DqeExchangeAbortAction();

  private DqeExchangeAbortAction() {
    super(NAME, DqeExchangeAbortResponse::new);
  }
}
