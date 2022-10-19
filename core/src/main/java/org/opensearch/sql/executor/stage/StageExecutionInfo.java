/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.stage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.executor.StateChangeListener;

@ToString
@Getter
@RequiredArgsConstructor
public class StageExecutionInfo {

  private StageExecutionState state;

  @Setter
  private StageExecutionStats stats;

  @Setter
  private Optional<StageExecutionFailureInfo> failureInfo;

  private List<StateChangeListener<StageExecutionState>> listeners = new ArrayList<>();

  @Getter
  @RequiredArgsConstructor
  public static class StageExecutionFailureInfo {
    private final Exception exception;
    private final String context;
  }

  public void setState(StageExecutionState state) {
    this.state = state;
    for (StateChangeListener<StageExecutionState> listener : listeners) {
      listener.stateChanged(this.state);
    }
  }

  public void register(StateChangeListener<StageExecutionState> listener) {
    listeners.add(listener);
  }
}
