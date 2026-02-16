/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

/** Join type for distributed hash join. */
public enum JoinType {
    INNER,
    LEFT,
    RIGHT,
    CROSS
}
