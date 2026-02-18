/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;

/** ActionType for shipping shuffled data between nodes during distributed query execution. */
public class CalciteShuffleAction extends ActionType<CalciteShuffleResponse> {
    public static final String NAME = "indices:data/read/opensearch/calcite/shuffle";
    public static final CalciteShuffleAction INSTANCE = new CalciteShuffleAction();

    private CalciteShuffleAction() {
        super(NAME, CalciteShuffleResponse::new);
    }
}
