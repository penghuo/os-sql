/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.util.collection.unsafe.sort;

import java.io.IOException;

public abstract class UnsafeSorterIterator {

  public abstract boolean hasNext();

  public abstract void loadNext() throws IOException;

  public abstract Object getBaseObject();

  public abstract long getBaseOffset();

  public abstract int getRecordLength();

  public abstract long getKeyPrefix();

  public abstract int getNumRecords();

  public abstract long getCurrentPageNumber();
}
