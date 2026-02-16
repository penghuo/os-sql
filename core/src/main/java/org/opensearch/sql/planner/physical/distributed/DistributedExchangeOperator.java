/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.distributed.ExchangeSpec;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * Consumer-side exchange operator for distributed query execution. Reads rows from an {@link
 * ExchangeBuffer} that is populated by the transport layer from remote source fragments.
 *
 * <p>This operator is a leaf node in the consumer fragment's physical plan tree. The actual data
 * transfer is handled by the transport layer (ExchangeDataAction), which feeds rows into the buffer
 * from remote shard execution results.
 */
public class DistributedExchangeOperator extends PhysicalPlan {

    @Getter private final ExchangeSpec exchangeSpec;
    @Getter private final int sourceFragmentCount;

    private ExchangeBuffer buffer;

    /**
     * Creates a new exchange operator.
     *
     * @param exchangeSpec the exchange specification (type, partition keys, buffer size)
     * @param sourceFragmentCount the number of upstream source fragments feeding this exchange
     */
    public DistributedExchangeOperator(ExchangeSpec exchangeSpec, int sourceFragmentCount) {
        this.exchangeSpec = exchangeSpec;
        this.sourceFragmentCount = sourceFragmentCount;
    }

    @Override
    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitDistributedExchange(this, context);
    }

    @Override
    public void open() {
        buffer = new ExchangeBuffer(exchangeSpec.getBufferSizeBytes(), sourceFragmentCount);
    }

    @Override
    public boolean hasNext() {
        return buffer.hasNext();
    }

    @Override
    public ExprValue next() {
        return buffer.next();
    }

    @Override
    public void close() {
        if (buffer != null) {
            buffer.close();
        }
    }

    @Override
    public List<PhysicalPlan> getChild() {
        return Collections.emptyList();
    }

    /**
     * Returns the exchange buffer for external data feeding. The transport layer uses this to push
     * rows received from remote source fragments.
     *
     * @return the exchange buffer, or null if the operator has not been opened
     */
    public ExchangeBuffer getBuffer() {
        return buffer;
    }
}
