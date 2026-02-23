/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

/** Thrown when a referenced table/index does not exist in the cluster state. */
public class DqeTableNotFoundException extends DqeException {

  private final String tableName;

  public DqeTableNotFoundException(String tableName) {
    super("Table not found: " + tableName, DqeErrorCode.TABLE_NOT_FOUND);
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }
}
