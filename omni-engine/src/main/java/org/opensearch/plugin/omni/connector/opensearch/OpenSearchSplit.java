/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.SplitWeight;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class OpenSearchSplit
        implements ConnectorSplit
{
    private final String index;
    private final int shard;
    private final String segmentName;   // null means whole shard (no segment targeting)
    private final int minDoc;           // inclusive, -1 means no doc range filter
    private final int maxDoc;           // exclusive, -1 means no doc range filter
    private final long estimatedDocCount;
    private final List<HostAddress> addresses;

    @JsonCreator
    public OpenSearchSplit(
            @JsonProperty("index") String index,
            @JsonProperty("shard") int shard,
            @JsonProperty("segmentName") String segmentName,
            @JsonProperty("minDoc") int minDoc,
            @JsonProperty("maxDoc") int maxDoc,
            @JsonProperty("estimatedDocCount") long estimatedDocCount,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.index = requireNonNull(index, "index is null");
        this.shard = shard;
        this.segmentName = segmentName;
        this.minDoc = minDoc;
        this.maxDoc = maxDoc;
        this.estimatedDocCount = estimatedDocCount;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    // Convenience: shard-level split (no segment/doc range targeting)
    public static OpenSearchSplit shardSplit(String index, int shard, long docCount, List<HostAddress> addresses)
    {
        return new OpenSearchSplit(index, shard, null, -1, -1, docCount, addresses);
    }

    @JsonProperty
    public String getIndex() { return index; }

    @JsonProperty
    public int getShard() { return shard; }

    @JsonProperty
    public String getSegmentName() { return segmentName; }

    @JsonProperty
    public int getMinDoc() { return minDoc; }

    @JsonProperty
    public int getMaxDoc() { return maxDoc; }

    @JsonProperty
    public long getEstimatedDocCount() { return estimatedDocCount; }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    public boolean hasSegmentTarget()
    {
        return segmentName != null;
    }

    public boolean hasDocRangeTarget()
    {
        return minDoc >= 0 && maxDoc >= 0;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false; // Must run on node with shard data
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        // Weight proportional to doc count; clamped to [0.01, 1.0]
        return SplitWeight.fromProportion(Math.max(0.01, Math.min(1.0,
                estimatedDocCount / 100_000.0)));
    }

    @Override
    public Object getInfo()
    {
        return toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return index.length() * 2L + addresses.size() * 32L + 64L;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("opensearch{" + index + "/shard=" + shard);
        if (hasSegmentTarget()) sb.append("/seg=").append(segmentName);
        if (hasDocRangeTarget()) sb.append("/docs=[").append(minDoc).append(",").append(maxDoc).append(")");
        sb.append("/~").append(estimatedDocCount).append("docs}");
        return sb.toString();
    }
}
