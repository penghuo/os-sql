/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import org.opensearch.action.ActionType;

/**
 * Transport action for pushing exchange data pages from producer to consumer.
 *
 * <p>Action name: {@code internal:dqe/exchange/push}
 */
public class DqeExchangePushAction extends ActionType<DqeExchangePushResponse> {

  public static final String NAME = "internal:dqe/exchange/push";
  public static final DqeExchangePushAction INSTANCE = new DqeExchangePushAction();

  private DqeExchangePushAction() {
    super(NAME, DqeExchangePushResponse::new);
  }
}
