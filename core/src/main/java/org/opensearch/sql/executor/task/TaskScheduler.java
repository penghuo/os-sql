/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.task;

import java.util.List;
import java.util.Set;
import org.opensearch.sql.planner.logical.LogicalPlan;

public interface TaskScheduler {

  List<Task> schedule(LogicalPlan plan, Set<TaskId> setTasks);
}
