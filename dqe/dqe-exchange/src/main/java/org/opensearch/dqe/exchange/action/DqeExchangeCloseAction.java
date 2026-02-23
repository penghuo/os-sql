/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import org.opensearch.action.ActionType;

/**
 * Transport action for a producer signaling completion of its data stream.
 *
 * <p>Action name: {@code internal:dqe/exchange/close}
 */
public class DqeExchangeCloseAction extends ActionType<DqeExchangeCloseResponse> {

  public static final String NAME = "internal:dqe/exchange/close";
  public static final DqeExchangeCloseAction INSTANCE = new DqeExchangeCloseAction();

  private DqeExchangeCloseAction() {
    super(NAME, DqeExchangeCloseResponse::new);
  }
}
