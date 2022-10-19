/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage.splits;

import java.io.IOException;

public abstract class Split {

  public Split() {
    // do nothing
  }

//  public Split(String json) {
//    // do nothing
//  }

  public abstract boolean onLocalNode();

//  public abstract String toJson();
}
