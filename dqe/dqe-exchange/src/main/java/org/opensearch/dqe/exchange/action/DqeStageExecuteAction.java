/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import org.opensearch.action.ActionType;

/**
 * Transport action for dispatching a plan fragment (stage) to a data node for execution.
 *
 * <p>Action name: {@code internal:dqe/stage/execute}
 */
public class DqeStageExecuteAction extends ActionType<DqeStageExecuteResponse> {

  public static final String NAME = "internal:dqe/stage/execute";
  public static final DqeStageExecuteAction INSTANCE = new DqeStageExecuteAction();

  private DqeStageExecuteAction() {
    super(NAME, DqeStageExecuteResponse::new);
  }
}
