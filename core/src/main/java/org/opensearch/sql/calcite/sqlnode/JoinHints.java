/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.Set;

/**
 * Bind-bare-to-LEFT semantics state for the current join scope. When an upstream {@code visitJoin}
 * leaves alias scope live for the next pipe (the explicit-ON path), a downstream pipe can resolve
 * {@code a.name} or a bare {@code name} via {@link Frame#joinHints}. The three fields move as a
 * unit — wrap clears them, the explicit-ON visitJoin sets them — so they're packaged here as a
 * record.
 */
record JoinHints(String leftAlias, String rightAlias, Set<String> ambiguousColumns) {}
