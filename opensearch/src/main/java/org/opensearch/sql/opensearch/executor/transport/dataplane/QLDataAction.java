/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.executor.transport.dataplane;

import org.opensearch.action.ActionType;

public class QLDataAction extends ActionType<QLDataResponse> {

  public static final QLDataAction INSTANCE = new QLDataAction();
  public static final String NAME = "cluster:admin/opensearch/ql/data";

  private QLDataAction() {
    super(NAME, QLDataResponse::new);
  }
}
