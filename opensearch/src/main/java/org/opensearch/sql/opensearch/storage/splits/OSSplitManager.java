/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.storage.splits;

import java.util.Collections;
import java.util.List;
import org.opensearch.sql.storage.splits.Split;
import org.opensearch.sql.storage.splits.SplitManager;

public class OSSplitManager implements SplitManager {
  @Override
  public List<Split> nextBatch() {
    return Collections.singletonList(new OpenSearchSplit());
  }

  @Override
  public void close() {

  }
}
