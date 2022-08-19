/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.planner.splits.Split;

@RequiredArgsConstructor
public class TaskNode {

  @Getter
  private final DiscoveryNode node;

  @Getter
  private final Type type;

  @Getter
  private final Split split;

  public boolean isLocalNode() {
    return type == Type.LOCAL;
  }

  public enum Type {
    LOCAL,
    OTHER;
  }
}
