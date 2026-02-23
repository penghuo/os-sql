/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.pit;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.common.unit.TimeValue;

/** Handle representing a Point-in-Time snapshot for consistent query execution. */
public class PitHandle {

  private final String pitId;
  private final String indexName;
  private final TimeValue keepAlive;
  private final AtomicBoolean released = new AtomicBoolean(false);

  public PitHandle(String pitId, String indexName, TimeValue keepAlive) {
    this.pitId = Objects.requireNonNull(pitId, "pitId must not be null");
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.keepAlive = Objects.requireNonNull(keepAlive, "keepAlive must not be null");
  }

  public String getPitId() {
    return pitId;
  }

  public String getIndexName() {
    return indexName;
  }

  public TimeValue getKeepAlive() {
    return keepAlive;
  }

  /** Returns true if this PIT has been released. */
  public boolean isReleased() {
    return released.get();
  }

  /** Marks this PIT as released. */
  void markReleased() {
    released.set(true);
  }

  @Override
  public String toString() {
    return "PitHandle{" + indexName + ", pitId=" + pitId + ", released=" + released.get() + "}";
  }
}
