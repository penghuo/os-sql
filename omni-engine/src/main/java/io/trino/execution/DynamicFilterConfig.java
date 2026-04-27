/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

@DefunctConfig({
        "dynamic-filtering-max-per-driver-row-count",
        "experimental.dynamic-filtering-max-per-driver-row-count",
        "dynamic-filtering-max-per-driver-size",
        "experimental.dynamic-filtering-max-per-driver-size",
        "dynamic-filtering-range-row-limit-per-driver",
        "experimental.dynamic-filtering-refresh-interval",
        "dynamic-filtering.service-thread-count"
})
public class DynamicFilterConfig
{
    private boolean enableDynamicFiltering = true;
    private boolean enableCoordinatorDynamicFiltersDistribution = true;
    private boolean enableLargeDynamicFilters;

    /*
     * dynamic-filtering.small.* and dynamic-filtering.large.* limits are applied when
     * collected over a not pre-partitioned source (when join distribution type is
     * REPLICATED or when FTE is enabled).
     *
     * dynamic-filtering.small-partitioned.* and dynamic-filtering.large-partitioned.*
     * limits are applied when collected over a pre-partitioned source (when join
     * distribution type is PARTITIONED and FTE is disabled).
     *
     * When FTE is enabled dynamic filters are always collected over non partitioned data,
     * hence the dynamic-filtering.small.* and dynamic-filtering.large.* limits applied.
     */
    private int smallMaxDistinctValuesPerDriver = 1_000;
    private DataSize smallMaxSizePerDriver = DataSize.of(100, KILOBYTE);
    private int smallRangeRowLimitPerDriver = 2_000;
    private DataSize smallMaxSizePerOperator = DataSize.of(1, MEGABYTE);
    private int smallPartitionedMaxDistinctValuesPerDriver = 100;
    private DataSize smallPartitionedMaxSizePerDriver = DataSize.of(50, KILOBYTE);
    private int smallPartitionedRangeRowLimitPerDriver = 500;
    private DataSize smallPartitionedMaxSizePerOperator = DataSize.of(500, KILOBYTE);
    private DataSize smallMaxSizePerFilter = DataSize.of(5, MEGABYTE);

    private int largeMaxDistinctValuesPerDriver = 10_000;
    private DataSize largeMaxSizePerDriver = DataSize.of(2, MEGABYTE);
    private int largeRangeRowLimitPerDriver = 20_000;
    private DataSize largeMaxSizePerOperator = DataSize.of(5, MEGABYTE);
    private int largePartitionedMaxDistinctValuesPerDriver = 1_000;
    private DataSize largePartitionedMaxSizePerDriver = DataSize.of(200, KILOBYTE);
    private int largePartitionedRangeRowLimitPerDriver = 2_000;
    private DataSize largePartitionedMaxSizePerOperator = DataSize.of(2, MEGABYTE);
    private DataSize largeMaxSizePerFilter = DataSize.of(5, MEGABYTE);

    public boolean isEnableDynamicFiltering()
    {
        return enableDynamicFiltering;
    }

    @LegacyConfig("experimental.enable-dynamic-filtering")
    public DynamicFilterConfig setEnableDynamicFiltering(boolean enableDynamicFiltering)
    {
        this.enableDynamicFiltering = enableDynamicFiltering;
        return this;
    }

    public boolean isEnableCoordinatorDynamicFiltersDistribution()
    {
        return enableCoordinatorDynamicFiltersDistribution;
    }

    public DynamicFilterConfig setEnableCoordinatorDynamicFiltersDistribution(boolean enableCoordinatorDynamicFiltersDistribution)
    {
        this.enableCoordinatorDynamicFiltersDistribution = enableCoordinatorDynamicFiltersDistribution;
        return this;
    }

    public boolean isEnableLargeDynamicFilters()
    {
        return enableLargeDynamicFilters;
    }

    public DynamicFilterConfig setEnableLargeDynamicFilters(boolean enableLargeDynamicFilters)
    {
        this.enableLargeDynamicFilters = enableLargeDynamicFilters;
        return this;
    }

    @Min(0)
    public int getSmallMaxDistinctValuesPerDriver()
    {
        return smallMaxDistinctValuesPerDriver;
    }

    @LegacyConfig("dynamic-filtering.small-broadcast.max-distinct-values-per-driver")
    public DynamicFilterConfig setSmallMaxDistinctValuesPerDriver(int smallMaxDistinctValuesPerDriver)
    {
        this.smallMaxDistinctValuesPerDriver = smallMaxDistinctValuesPerDriver;
        return this;
    }

    @MaxDataSize("1MB")
    public DataSize getSmallMaxSizePerDriver()
    {
        return smallMaxSizePerDriver;
    }

    @LegacyConfig("dynamic-filtering.small-broadcast.max-size-per-driver")
    public DynamicFilterConfig setSmallMaxSizePerDriver(DataSize smallMaxSizePerDriver)
    {
        this.smallMaxSizePerDriver = smallMaxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getSmallRangeRowLimitPerDriver()
    {
        return smallRangeRowLimitPerDriver;
    }

    @LegacyConfig("dynamic-filtering.small-broadcast.range-row-limit-per-driver")
    public DynamicFilterConfig setSmallRangeRowLimitPerDriver(int smallRangeRowLimitPerDriver)
    {
        this.smallRangeRowLimitPerDriver = smallRangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("10MB")
    public DataSize getSmallMaxSizePerOperator()
    {
        return smallMaxSizePerOperator;
    }

    @LegacyConfig("dynamic-filtering.small-broadcast.max-size-per-operator")
    public DynamicFilterConfig setSmallMaxSizePerOperator(DataSize smallMaxSizePerOperator)
    {
        this.smallMaxSizePerOperator = smallMaxSizePerOperator;
        return this;
    }

    @Min(0)
    public int getSmallPartitionedMaxDistinctValuesPerDriver()
    {
        return smallPartitionedMaxDistinctValuesPerDriver;
    }

    public DynamicFilterConfig setSmallPartitionedMaxDistinctValuesPerDriver(int smallPartitionedMaxDistinctValuesPerDriver)
    {
        this.smallPartitionedMaxDistinctValuesPerDriver = smallPartitionedMaxDistinctValuesPerDriver;
        return this;
    }

    @MaxDataSize("1MB")
    public DataSize getSmallPartitionedMaxSizePerDriver()
    {
        return smallPartitionedMaxSizePerDriver;
    }

    public DynamicFilterConfig setSmallPartitionedMaxSizePerDriver(DataSize smallPartitionedMaxSizePerDriver)
    {
        this.smallPartitionedMaxSizePerDriver = smallPartitionedMaxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getSmallPartitionedRangeRowLimitPerDriver()
    {
        return smallPartitionedRangeRowLimitPerDriver;
    }

    public DynamicFilterConfig setSmallPartitionedRangeRowLimitPerDriver(int smallPartitionedRangeRowLimitPerDriver)
    {
        this.smallPartitionedRangeRowLimitPerDriver = smallPartitionedRangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("10MB")
    public DataSize getSmallPartitionedMaxSizePerOperator()
    {
        return smallPartitionedMaxSizePerOperator;
    }

    public DynamicFilterConfig setSmallPartitionedMaxSizePerOperator(DataSize smallPartitionedMaxSizePerOperator)
    {
        this.smallPartitionedMaxSizePerOperator = smallPartitionedMaxSizePerOperator;
        return this;
    }

    @NotNull
    @MaxDataSize("10MB")
    public DataSize getSmallMaxSizePerFilter()
    {
        return smallMaxSizePerFilter;
    }

    public DynamicFilterConfig setSmallMaxSizePerFilter(DataSize smallMaxSizePerFilter)
    {
        this.smallMaxSizePerFilter = smallMaxSizePerFilter;
        return this;
    }

    @Min(0)
    public int getLargeMaxDistinctValuesPerDriver()
    {
        return largeMaxDistinctValuesPerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.max-distinct-values-per-driver")
    public DynamicFilterConfig setLargeMaxDistinctValuesPerDriver(int largeMaxDistinctValuesPerDriver)
    {
        this.largeMaxDistinctValuesPerDriver = largeMaxDistinctValuesPerDriver;
        return this;
    }

    public DataSize getLargeMaxSizePerDriver()
    {
        return largeMaxSizePerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.max-size-per-driver")
    public DynamicFilterConfig setLargeMaxSizePerDriver(DataSize largeMaxSizePerDriver)
    {
        this.largeMaxSizePerDriver = largeMaxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getLargeRangeRowLimitPerDriver()
    {
        return largeRangeRowLimitPerDriver;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.range-row-limit-per-driver")
    public DynamicFilterConfig setLargeRangeRowLimitPerDriver(int largeRangeRowLimitPerDriver)
    {
        this.largeRangeRowLimitPerDriver = largeRangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("100MB")
    public DataSize getLargeMaxSizePerOperator()
    {
        return largeMaxSizePerOperator;
    }

    @LegacyConfig("dynamic-filtering.large-broadcast.max-size-per-operator")
    public DynamicFilterConfig setLargeMaxSizePerOperator(DataSize largeMaxSizePerOperator)
    {
        this.largeMaxSizePerOperator = largeMaxSizePerOperator;
        return this;
    }

    @Min(0)
    public int getLargePartitionedMaxDistinctValuesPerDriver()
    {
        return largePartitionedMaxDistinctValuesPerDriver;
    }

    public DynamicFilterConfig setLargePartitionedMaxDistinctValuesPerDriver(int largePartitionedMaxDistinctValuesPerDriver)
    {
        this.largePartitionedMaxDistinctValuesPerDriver = largePartitionedMaxDistinctValuesPerDriver;
        return this;
    }

    public DataSize getLargePartitionedMaxSizePerDriver()
    {
        return largePartitionedMaxSizePerDriver;
    }

    public DynamicFilterConfig setLargePartitionedMaxSizePerDriver(DataSize largePartitionedMaxSizePerDriver)
    {
        this.largePartitionedMaxSizePerDriver = largePartitionedMaxSizePerDriver;
        return this;
    }

    @Min(0)
    public int getLargePartitionedRangeRowLimitPerDriver()
    {
        return largePartitionedRangeRowLimitPerDriver;
    }

    public DynamicFilterConfig setLargePartitionedRangeRowLimitPerDriver(int largePartitionedRangeRowLimitPerDriver)
    {
        this.largePartitionedRangeRowLimitPerDriver = largePartitionedRangeRowLimitPerDriver;
        return this;
    }

    @MaxDataSize("50MB")
    public DataSize getLargePartitionedMaxSizePerOperator()
    {
        return largePartitionedMaxSizePerOperator;
    }

    public DynamicFilterConfig setLargePartitionedMaxSizePerOperator(DataSize largePartitionedMaxSizePerOperator)
    {
        this.largePartitionedMaxSizePerOperator = largePartitionedMaxSizePerOperator;
        return this;
    }

    @NotNull
    @MaxDataSize("10MB")
    public DataSize getLargeMaxSizePerFilter()
    {
        return largeMaxSizePerFilter;
    }

    public DynamicFilterConfig setLargeMaxSizePerFilter(DataSize largeMaxSizePerFilter)
    {
        this.largeMaxSizePerFilter = largeMaxSizePerFilter;
        return this;
    }
}
