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
package io.trino.sql.planner;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig({"adaptive-partial-aggregation.min-rows", "preferred-write-partitioning-min-number-of-partitions", "optimizer.use-mark-distinct"})
public class OptimizerConfig
{
    private double cpuCostWeight = 75;
    private double memoryCostWeight = 10;
    private double networkCostWeight = 15;

    private DataSize joinMaxBroadcastTableSize = DataSize.of(100, MEGABYTE);
    private JoinDistributionType joinDistributionType = JoinDistributionType.AUTOMATIC;
    private double joinMultiClauseIndependenceFactor = 0.25;

    private JoinReorderingStrategy joinReorderingStrategy = JoinReorderingStrategy.AUTOMATIC;
    private int maxReorderedJoins = 9;
    private int maxPrefetchedInformationSchemaPrefixes = 100;

    private boolean enableStatsCalculator = true;
    private boolean statisticsPrecalculationForPushdownEnabled = true;
    private boolean collectPlanStatisticsForAllQueries;
    private boolean ignoreStatsCalculatorFailures = true;
    private boolean defaultFilterFactorEnabled;
    private double filterConjunctionIndependenceFactor = 0.75;
    private boolean nonEstimatablePredicateApproximationEnabled = true;

    private boolean colocatedJoinsEnabled = true;
    private boolean spatialJoinsEnabled = true;
    private boolean distributedSort = true;

    private boolean usePreferredWritePartitioning = true;

    private Duration iterativeOptimizerTimeout = new Duration(3, MINUTES); // by default let optimizer wait a long time in case it retrieves some data from ConnectorMetadata

    private boolean optimizeMetadataQueries;
    private boolean optimizeHashGeneration;
    private boolean pushTableWriteThroughUnion = true;
    private boolean dictionaryAggregation;
    private MarkDistinctStrategy markDistinctStrategy = MarkDistinctStrategy.AUTOMATIC;
    private boolean preferPartialAggregation = true;
    private boolean pushAggregationThroughOuterJoin = true;
    private boolean enableIntermediateAggregations;
    private boolean pushPartialAggregationThroughJoin;
    private boolean preAggregateCaseAggregationsEnabled = true;
    private boolean optimizeMixedDistinctAggregations;
    private boolean enableForcedExchangeBelowGroupId = true;
    private boolean optimizeTopNRanking = true;
    private boolean skipRedundantSort = true;
    private boolean complexExpressionPushdownEnabled = true;
    private boolean predicatePushdownUseTableProperties = true;
    private boolean ignoreDownstreamPreferences;
    private boolean rewriteFilteringSemiJoinToInnerJoin = true;
    private boolean optimizeDuplicateInsensitiveJoins = true;
    private boolean useLegacyWindowFilterPushdown;
    private boolean useTableScanNodePartitioning = true;
    private double tableScanNodePartitioningMinBucketToTaskRatio = 0.5;
    private boolean mergeProjectWithValues = true;
    private boolean forceSingleNodeOutput;
    private boolean useExactPartitioning;
    private boolean useCostBasedPartitioning = true;
    // adaptive partial aggregation
    private boolean adaptivePartialAggregationEnabled = true;
    private double adaptivePartialAggregationUniqueRowsRatioThreshold = 0.8;
    private long joinPartitionedBuildMinRowCount = 1_000_000L;
    private DataSize minInputSizePerTask = DataSize.of(5, GIGABYTE);
    private long minInputRowsPerTask = 10_000_000L;

    public enum JoinReorderingStrategy
    {
        NONE,
        ELIMINATE_CROSS_JOINS,
        AUTOMATIC,
    }

    public enum JoinDistributionType
    {
        BROADCAST,
        PARTITIONED,
        AUTOMATIC;

        public boolean canPartition()
        {
            return this == PARTITIONED || this == AUTOMATIC;
        }

        public boolean canReplicate()
        {
            return this == BROADCAST || this == AUTOMATIC;
        }
    }

    public enum MarkDistinctStrategy
    {
        NONE,
        ALWAYS,
        AUTOMATIC,
    }

    public double getCpuCostWeight()
    {
        return cpuCostWeight;
    }

    public OptimizerConfig setCpuCostWeight(double cpuCostWeight)
    {
        this.cpuCostWeight = cpuCostWeight;
        return this;
    }

    public double getMemoryCostWeight()
    {
        return memoryCostWeight;
    }

    public OptimizerConfig setMemoryCostWeight(double memoryCostWeight)
    {
        this.memoryCostWeight = memoryCostWeight;
        return this;
    }

    public double getNetworkCostWeight()
    {
        return networkCostWeight;
    }

    public OptimizerConfig setNetworkCostWeight(double networkCostWeight)
    {
        this.networkCostWeight = networkCostWeight;
        return this;
    }

    public JoinDistributionType getJoinDistributionType()
    {
        return joinDistributionType;
    }

    public OptimizerConfig setJoinDistributionType(JoinDistributionType joinDistributionType)
    {
        this.joinDistributionType = requireNonNull(joinDistributionType, "joinDistributionType is null");
        return this;
    }

    @NotNull
    public DataSize getJoinMaxBroadcastTableSize()
    {
        return joinMaxBroadcastTableSize;
    }

    public OptimizerConfig setJoinMaxBroadcastTableSize(DataSize joinMaxBroadcastTableSize)
    {
        this.joinMaxBroadcastTableSize = joinMaxBroadcastTableSize;
        return this;
    }

    @Min(0)
    @Max(1)
    public double getJoinMultiClauseIndependenceFactor()
    {
        return joinMultiClauseIndependenceFactor;
    }

    public OptimizerConfig setJoinMultiClauseIndependenceFactor(double joinMultiClauseIndependenceFactor)
    {
        this.joinMultiClauseIndependenceFactor = joinMultiClauseIndependenceFactor;
        return this;
    }

    public JoinReorderingStrategy getJoinReorderingStrategy()
    {
        return joinReorderingStrategy;
    }

    public OptimizerConfig setJoinReorderingStrategy(JoinReorderingStrategy joinReorderingStrategy)
    {
        this.joinReorderingStrategy = joinReorderingStrategy;
        return this;
    }

    @Min(2)
    public int getMaxReorderedJoins()
    {
        return maxReorderedJoins;
    }

    public OptimizerConfig setMaxReorderedJoins(int maxReorderedJoins)
    {
        this.maxReorderedJoins = maxReorderedJoins;
        return this;
    }

    @Min(1)
    public int getMaxPrefetchedInformationSchemaPrefixes()
    {
        return maxPrefetchedInformationSchemaPrefixes;
    }

    @ConfigHidden
    public OptimizerConfig setMaxPrefetchedInformationSchemaPrefixes(int maxPrefetchedInformationSchemaPrefixes)
    {
        this.maxPrefetchedInformationSchemaPrefixes = maxPrefetchedInformationSchemaPrefixes;
        return this;
    }

    public boolean isEnableStatsCalculator()
    {
        return enableStatsCalculator;
    }

    @LegacyConfig("experimental.enable-stats-calculator")
    public OptimizerConfig setEnableStatsCalculator(boolean enableStatsCalculator)
    {
        this.enableStatsCalculator = enableStatsCalculator;
        return this;
    }

    public boolean isStatisticsPrecalculationForPushdownEnabled()
    {
        return statisticsPrecalculationForPushdownEnabled;
    }

    public OptimizerConfig setStatisticsPrecalculationForPushdownEnabled(boolean statisticsPrecalculationForPushdownEnabled)
    {
        this.statisticsPrecalculationForPushdownEnabled = statisticsPrecalculationForPushdownEnabled;
        return this;
    }

    public boolean isCollectPlanStatisticsForAllQueries()
    {
        return collectPlanStatisticsForAllQueries;
    }

    public OptimizerConfig setCollectPlanStatisticsForAllQueries(boolean collectPlanStatisticsForAllQueries)
    {
        this.collectPlanStatisticsForAllQueries = collectPlanStatisticsForAllQueries;
        return this;
    }

    public boolean isIgnoreStatsCalculatorFailures()
    {
        return ignoreStatsCalculatorFailures;
    }

    public OptimizerConfig setIgnoreStatsCalculatorFailures(boolean ignoreStatsCalculatorFailures)
    {
        this.ignoreStatsCalculatorFailures = ignoreStatsCalculatorFailures;
        return this;
    }

    public boolean isDefaultFilterFactorEnabled()
    {
        return defaultFilterFactorEnabled;
    }

    public OptimizerConfig setDefaultFilterFactorEnabled(boolean defaultFilterFactorEnabled)
    {
        this.defaultFilterFactorEnabled = defaultFilterFactorEnabled;
        return this;
    }

    @Min(0)
    @Max(1)
    public double getFilterConjunctionIndependenceFactor()
    {
        return filterConjunctionIndependenceFactor;
    }

    public OptimizerConfig setFilterConjunctionIndependenceFactor(double filterConjunctionIndependenceFactor)
    {
        this.filterConjunctionIndependenceFactor = filterConjunctionIndependenceFactor;
        return this;
    }

    public boolean isNonEstimatablePredicateApproximationEnabled()
    {
        return nonEstimatablePredicateApproximationEnabled;
    }

    public OptimizerConfig setNonEstimatablePredicateApproximationEnabled(boolean nonEstimatablePredicateApproximationEnabled)
    {
        this.nonEstimatablePredicateApproximationEnabled = nonEstimatablePredicateApproximationEnabled;
        return this;
    }

    public boolean isColocatedJoinsEnabled()
    {
        return colocatedJoinsEnabled;
    }

    public OptimizerConfig setColocatedJoinsEnabled(boolean colocatedJoinsEnabled)
    {
        this.colocatedJoinsEnabled = colocatedJoinsEnabled;
        return this;
    }

    public boolean isSpatialJoinsEnabled()
    {
        return spatialJoinsEnabled;
    }

    public OptimizerConfig setSpatialJoinsEnabled(boolean spatialJoinsEnabled)
    {
        this.spatialJoinsEnabled = spatialJoinsEnabled;
        return this;
    }

    public boolean isDistributedSortEnabled()
    {
        return distributedSort;
    }

    public OptimizerConfig setDistributedSortEnabled(boolean enabled)
    {
        distributedSort = enabled;
        return this;
    }

    public boolean isUsePreferredWritePartitioning()
    {
        return usePreferredWritePartitioning;
    }

    public OptimizerConfig setUsePreferredWritePartitioning(boolean usePreferredWritePartitioning)
    {
        this.usePreferredWritePartitioning = usePreferredWritePartitioning;
        return this;
    }

    public Duration getIterativeOptimizerTimeout()
    {
        return iterativeOptimizerTimeout;
    }

    @LegacyConfig("experimental.iterative-optimizer-timeout")
    public OptimizerConfig setIterativeOptimizerTimeout(Duration timeout)
    {
        this.iterativeOptimizerTimeout = timeout;
        return this;
    }

    public boolean isOptimizeMixedDistinctAggregations()
    {
        return optimizeMixedDistinctAggregations;
    }

    public OptimizerConfig setOptimizeMixedDistinctAggregations(boolean value)
    {
        this.optimizeMixedDistinctAggregations = value;
        return this;
    }

    public boolean isEnableIntermediateAggregations()
    {
        return enableIntermediateAggregations;
    }

    public OptimizerConfig setEnableIntermediateAggregations(boolean enableIntermediateAggregations)
    {
        this.enableIntermediateAggregations = enableIntermediateAggregations;
        return this;
    }

    public boolean isPushAggregationThroughOuterJoin()
    {
        return pushAggregationThroughOuterJoin;
    }

    @LegacyConfig("optimizer.push-aggregation-through-join")
    public OptimizerConfig setPushAggregationThroughOuterJoin(boolean pushAggregationThroughOuterJoin)
    {
        this.pushAggregationThroughOuterJoin = pushAggregationThroughOuterJoin;
        return this;
    }

    public boolean isPushPartialAggregationThroughJoin()
    {
        return pushPartialAggregationThroughJoin;
    }

    public OptimizerConfig setPushPartialAggregationThroughJoin(boolean pushPartialAggregationThroughJoin)
    {
        this.pushPartialAggregationThroughJoin = pushPartialAggregationThroughJoin;
        return this;
    }

    public boolean isPreAggregateCaseAggregationsEnabled()
    {
        return preAggregateCaseAggregationsEnabled;
    }

    public OptimizerConfig setPreAggregateCaseAggregationsEnabled(boolean preAggregateCaseAggregationsEnabled)
    {
        this.preAggregateCaseAggregationsEnabled = preAggregateCaseAggregationsEnabled;
        return this;
    }

    public boolean isOptimizeMetadataQueries()
    {
        return optimizeMetadataQueries;
    }

    public OptimizerConfig setOptimizeMetadataQueries(boolean optimizeMetadataQueries)
    {
        this.optimizeMetadataQueries = optimizeMetadataQueries;
        return this;
    }

    @Nullable
    public MarkDistinctStrategy getMarkDistinctStrategy()
    {
        return markDistinctStrategy;
    }

    public OptimizerConfig setMarkDistinctStrategy(MarkDistinctStrategy markDistinctStrategy)
    {
        this.markDistinctStrategy = markDistinctStrategy;
        return this;
    }

    public boolean isPreferPartialAggregation()
    {
        return preferPartialAggregation;
    }

    public OptimizerConfig setPreferPartialAggregation(boolean value)
    {
        this.preferPartialAggregation = value;
        return this;
    }

    public boolean isEnableForcedExchangeBelowGroupId()
    {
        return enableForcedExchangeBelowGroupId;
    }

    public OptimizerConfig setEnableForcedExchangeBelowGroupId(boolean enableForcedExchangeBelowGroupId)
    {
        this.enableForcedExchangeBelowGroupId = enableForcedExchangeBelowGroupId;
        return this;
    }

    public boolean isOptimizeTopNRanking()
    {
        return optimizeTopNRanking;
    }

    @LegacyConfig("optimizer.optimize-top-n-row-number")
    public OptimizerConfig setOptimizeTopNRanking(boolean optimizeTopNRanking)
    {
        this.optimizeTopNRanking = optimizeTopNRanking;
        return this;
    }

    public boolean isOptimizeHashGeneration()
    {
        return optimizeHashGeneration;
    }

    public OptimizerConfig setOptimizeHashGeneration(boolean optimizeHashGeneration)
    {
        this.optimizeHashGeneration = optimizeHashGeneration;
        return this;
    }

    public boolean isPushTableWriteThroughUnion()
    {
        return pushTableWriteThroughUnion;
    }

    public OptimizerConfig setPushTableWriteThroughUnion(boolean pushTableWriteThroughUnion)
    {
        this.pushTableWriteThroughUnion = pushTableWriteThroughUnion;
        return this;
    }

    public boolean isDictionaryAggregation()
    {
        return dictionaryAggregation;
    }

    public OptimizerConfig setDictionaryAggregation(boolean dictionaryAggregation)
    {
        this.dictionaryAggregation = dictionaryAggregation;
        return this;
    }

    public boolean isSkipRedundantSort()
    {
        return skipRedundantSort;
    }

    public OptimizerConfig setSkipRedundantSort(boolean value)
    {
        this.skipRedundantSort = value;
        return this;
    }

    public boolean isComplexExpressionPushdownEnabled()
    {
        return complexExpressionPushdownEnabled;
    }

    public OptimizerConfig setComplexExpressionPushdownEnabled(boolean complexExpressionPushdownEnabled)
    {
        this.complexExpressionPushdownEnabled = complexExpressionPushdownEnabled;
        return this;
    }

    public boolean isPredicatePushdownUseTableProperties()
    {
        return predicatePushdownUseTableProperties;
    }

    public OptimizerConfig setPredicatePushdownUseTableProperties(boolean predicatePushdownUseTableProperties)
    {
        this.predicatePushdownUseTableProperties = predicatePushdownUseTableProperties;
        return this;
    }

    public boolean isIgnoreDownstreamPreferences()
    {
        return ignoreDownstreamPreferences;
    }

    public OptimizerConfig setIgnoreDownstreamPreferences(boolean ignoreDownstreamPreferences)
    {
        this.ignoreDownstreamPreferences = ignoreDownstreamPreferences;
        return this;
    }

    public boolean isRewriteFilteringSemiJoinToInnerJoin()
    {
        return rewriteFilteringSemiJoinToInnerJoin;
    }

    public OptimizerConfig setRewriteFilteringSemiJoinToInnerJoin(boolean rewriteFilteringSemiJoinToInnerJoin)
    {
        this.rewriteFilteringSemiJoinToInnerJoin = rewriteFilteringSemiJoinToInnerJoin;
        return this;
    }

    public boolean isOptimizeDuplicateInsensitiveJoins()
    {
        return optimizeDuplicateInsensitiveJoins;
    }

    public OptimizerConfig setOptimizeDuplicateInsensitiveJoins(boolean optimizeDuplicateInsensitiveJoins)
    {
        this.optimizeDuplicateInsensitiveJoins = optimizeDuplicateInsensitiveJoins;
        return this;
    }

    public boolean isUseLegacyWindowFilterPushdown()
    {
        return useLegacyWindowFilterPushdown;
    }

    public OptimizerConfig setUseLegacyWindowFilterPushdown(boolean useLegacyWindowFilterPushdown)
    {
        this.useLegacyWindowFilterPushdown = useLegacyWindowFilterPushdown;
        return this;
    }

    public boolean isUseTableScanNodePartitioning()
    {
        return useTableScanNodePartitioning;
    }

    @LegacyConfig("optimizer.plan-with-table-node-partitioning")
    public OptimizerConfig setUseTableScanNodePartitioning(boolean useTableScanNodePartitioning)
    {
        this.useTableScanNodePartitioning = useTableScanNodePartitioning;
        return this;
    }

    @Min(0)
    public double getTableScanNodePartitioningMinBucketToTaskRatio()
    {
        return tableScanNodePartitioningMinBucketToTaskRatio;
    }

    public OptimizerConfig setTableScanNodePartitioningMinBucketToTaskRatio(double tableScanNodePartitioningMinBucketToTaskRatio)
    {
        this.tableScanNodePartitioningMinBucketToTaskRatio = tableScanNodePartitioningMinBucketToTaskRatio;
        return this;
    }

    public boolean isMergeProjectWithValues()
    {
        return mergeProjectWithValues;
    }

    public OptimizerConfig setMergeProjectWithValues(boolean mergeProjectWithValues)
    {
        this.mergeProjectWithValues = mergeProjectWithValues;
        return this;
    }

    public boolean isForceSingleNodeOutput()
    {
        return forceSingleNodeOutput;
    }

    public OptimizerConfig setForceSingleNodeOutput(boolean value)
    {
        this.forceSingleNodeOutput = value;
        return this;
    }

    public boolean isAdaptivePartialAggregationEnabled()
    {
        return adaptivePartialAggregationEnabled;
    }

    public OptimizerConfig setAdaptivePartialAggregationEnabled(boolean adaptivePartialAggregationEnabled)
    {
        this.adaptivePartialAggregationEnabled = adaptivePartialAggregationEnabled;
        return this;
    }

    public double getAdaptivePartialAggregationUniqueRowsRatioThreshold()
    {
        return adaptivePartialAggregationUniqueRowsRatioThreshold;
    }

    public OptimizerConfig setAdaptivePartialAggregationUniqueRowsRatioThreshold(double adaptivePartialAggregationUniqueRowsRatioThreshold)
    {
        this.adaptivePartialAggregationUniqueRowsRatioThreshold = adaptivePartialAggregationUniqueRowsRatioThreshold;
        return this;
    }

    @Min(0)
    public long getJoinPartitionedBuildMinRowCount()
    {
        return joinPartitionedBuildMinRowCount;
    }

    public OptimizerConfig setJoinPartitionedBuildMinRowCount(long joinPartitionedBuildMinRowCount)
    {
        this.joinPartitionedBuildMinRowCount = joinPartitionedBuildMinRowCount;
        return this;
    }

    @NotNull
    public DataSize getMinInputSizePerTask()
    {
        return minInputSizePerTask;
    }

    public OptimizerConfig setMinInputSizePerTask(DataSize minInputSizePerTask)
    {
        this.minInputSizePerTask = minInputSizePerTask;
        return this;
    }

    @Min(0)
    public long getMinInputRowsPerTask()
    {
        return minInputRowsPerTask;
    }

    public OptimizerConfig setMinInputRowsPerTask(long minInputRowsPerTask)
    {
        this.minInputRowsPerTask = minInputRowsPerTask;
        return this;
    }

    public boolean isUseExactPartitioning()
    {
        return useExactPartitioning;
    }

    public OptimizerConfig setUseExactPartitioning(boolean useExactPartitioning)
    {
        this.useExactPartitioning = useExactPartitioning;
        return this;
    }

    public boolean isUseCostBasedPartitioning()
    {
        return useCostBasedPartitioning;
    }

    public OptimizerConfig setUseCostBasedPartitioning(boolean useCostBasedPartitioning)
    {
        this.useCostBasedPartitioning = useCostBasedPartitioning;
        return this;
    }
}
