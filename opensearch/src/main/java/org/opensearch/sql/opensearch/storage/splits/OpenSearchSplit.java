/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.storage.splits;

import org.opensearch.sql.storage.splits.Split;

public class OpenSearchSplit extends Split {

  public OpenSearchSplit() {
  }

//  public OpenSearchSplit(String json) {
//    super(json);
//  }

  @Override
  public boolean onLocalNode() {
    return true;
  }

//  @Override
//  public String toJson() {
//    return "";
//  }
}
