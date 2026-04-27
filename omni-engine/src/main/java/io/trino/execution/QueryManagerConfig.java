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
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import io.trino.operator.RetryPolicy;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@DefunctConfig({
        "query.max-pending-splits-per-node",
        "query.queue-config-file",
        "experimental.big-query-initial-hash-partitions",
        "experimental.fault-tolerant-execution-force-preferred-write-partitioning-enabled",
        "experimental.max-concurrent-big-queries",
        "experimental.max-queued-big-queries",
        "query-manager.initialization-required-workers",
        "query-manager.initialization-timeout",
        "fault-tolerant-execution-target-task-split-count",
        "fault-tolerant-execution-target-task-input-size",
        "query.remote-task.max-consecutive-error-count",
        "query.remote-task.min-error-duration",
})
public class QueryManagerConfig
{
    public static final long AVAILABLE_HEAP_MEMORY = Runtime.getRuntime().maxMemory();
    public static final int MAX_TASK_RETRY_ATTEMPTS = 126;
    public static final int FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT_LIMIT = 1000;

    private int scheduleSplitBatchSize = 1000;
    private int minScheduleSplitBatchSize = 100;
    private int maxConcurrentQueries = 1000;
    private int maxQueuedQueries = 5000;

    private boolean determinePartitionCountForWriteEnabled;
    private int maxHashPartitionCount = 100;
    private int minHashPartitionCount = 4;
    private int minHashPartitionCountForWrite = 50;
    private int maxWriterTaskCount = 100;
    private Duration minQueryExpireAge = new Duration(15, TimeUnit.MINUTES);
    private int maxQueryHistory = 100;
    private int maxQueryLength = 1_000_000;
    private int maxStageCount = 150;
    private int stageCountWarningThreshold = 50;

    private Duration clientTimeout = new Duration(5, TimeUnit.MINUTES);

    private int queryManagerExecutorPoolSize = 5;
    private int queryExecutorPoolSize = 1000;
    private int maxStateMachineCallbackThreads = 5;
    private int maxSplitManagerCallbackThreads = 100;

    /**
     * default value is overwritten for fault tolerant execution in {@link #applyFaultTolerantExecutionDefaults()}
     */
    private Duration remoteTaskMaxErrorDuration = new Duration(5, TimeUnit.MINUTES);
    private int remoteTaskMaxCallbackThreads = 1000;

    private String queryExecutionPolicy = "phased";
    private Duration queryMaxRunTime = new Duration(100, TimeUnit.DAYS);
    private Duration queryMaxExecutionTime = new Duration(100, TimeUnit.DAYS);
    private Duration queryMaxPlanningTime = new Duration(10, TimeUnit.MINUTES);
    private Duration queryMaxCpuTime = new Duration(1_000_000_000, TimeUnit.DAYS);
    private Optional<DataSize> queryMaxScanPhysicalBytes = Optional.empty();
    private int queryReportedRuleStatsLimit = 10;

    private int requiredWorkers = 1;
    private Duration requiredWorkersMaxWait = new Duration(5, TimeUnit.MINUTES);

    private RetryPolicy retryPolicy = RetryPolicy.NONE;
    private int queryRetryAttempts = 4;
    private int taskRetryAttemptsPerTask = 4;
    private Duration retryInitialDelay = new Duration(10, SECONDS);
    private Duration retryMaxDelay = new Duration(1, MINUTES);
    private double retryDelayScaleFactor = 2.0;

    private int maxTasksWaitingForExecutionPerQuery = 10;
    private int maxTasksWaitingForNodePerStage = 5;

    private boolean enabledAdaptiveTaskRequestSize = true;
    private DataSize maxRemoteTaskRequestSize = DataSize.of(8, MEGABYTE);
    private DataSize remoteTaskRequestSizeHeadroom = DataSize.of(2, MEGABYTE);
    private int remoteTaskGuaranteedSplitPerTask = 3;

    private int faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod = 64;
    private double faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor = 1.26;
    private DataSize faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin = DataSize.of(512, MEGABYTE);
    private DataSize faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax = DataSize.of(50, GIGABYTE);

    private int faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod = 64;
    private double faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor = 1.26;
    private DataSize faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin = DataSize.of(4, GIGABYTE);
    private DataSize faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax = DataSize.of(50, GIGABYTE);

    private DataSize faultTolerantExecutionHashDistributionComputeTaskTargetSize = DataSize.of(512, MEGABYTE);
    private double faultTolerantExecutionHashDistributionComputeTasksToNodesMinRatio = 2.0;
    private DataSize faultTolerantExecutionHashDistributionWriteTaskTargetSize = DataSize.of(4, GIGABYTE);
    private double faultTolerantExecutionHashDistributionWriteTasksToNodesMinRatio = 2.0;
    private int faultTolerantExecutionHashDistributionWriteTaskTargetMaxCount = 2000;

    private DataSize faultTolerantExecutionStandardSplitSize = DataSize.of(64, MEGABYTE);
    private int faultTolerantExecutionMaxTaskSplitCount = 256;
    private DataSize faultTolerantExecutionTaskDescriptorStorageMaxMemory = DataSize.ofBytes(Math.round(AVAILABLE_HEAP_MEMORY * 0.15));
    private int faultTolerantExecutionMaxPartitionCount = 50;
    private int faultTolerantExecutionMinPartitionCount = 4;
    private int faultTolerantExecutionMinPartitionCountForWrite = 50;
    private boolean faultTolerantExecutionRuntimeAdaptivePartitioningEnabled;
    private int faultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount = FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT_LIMIT;
    // Currently, initial setup is 5GB of task memory processing 4GB data. Given that we triple the memory in case of
    // task OOM, max task size is set to 12GB such that tasks of stages below threshold will succeed within one retry.
    private DataSize faultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize = DataSize.of(12, GIGABYTE);
    private double faultTolerantExecutionMinSourceStageProgress = 0.2;

    private boolean faultTolerantExecutionSmallStageEstimationEnabled = true;
    private DataSize faultTolerantExecutionSmallStageEstimationThreshold = DataSize.of(20, GIGABYTE);
    private double faultTolerantExecutionSmallStageSourceSizeMultiplier = 1.2;
    private boolean faultTolerantExecutionSmallStageRequireNoMorePartitions;
    private boolean faultTolerantExecutionStageEstimationForEagerParentEnabled = true;
    private boolean faultTolerantExecutionAdaptiveQueryPlanningEnabled;

    @Min(1)
    public int getScheduleSplitBatchSize()
    {
        return scheduleSplitBatchSize;
    }

    public QueryManagerConfig setScheduleSplitBatchSize(int scheduleSplitBatchSize)
    {
        this.scheduleSplitBatchSize = scheduleSplitBatchSize;
        return this;
    }

    @Min(1)
    public int getMinScheduleSplitBatchSize()
    {
        return minScheduleSplitBatchSize;
    }

    public QueryManagerConfig setMinScheduleSplitBatchSize(int minScheduleSplitBatchSize)
    {
        this.minScheduleSplitBatchSize = minScheduleSplitBatchSize;
        return this;
    }

    @Deprecated
    @Min(1)
    public int getMaxConcurrentQueries()
    {
        return maxConcurrentQueries;
    }

    @Deprecated
    public QueryManagerConfig setMaxConcurrentQueries(int maxConcurrentQueries)
    {
        this.maxConcurrentQueries = maxConcurrentQueries;
        return this;
    }

    @Deprecated
    @Min(1)
    public int getMaxQueuedQueries()
    {
        return maxQueuedQueries;
    }

    @Deprecated
    public QueryManagerConfig setMaxQueuedQueries(int maxQueuedQueries)
    {
        this.maxQueuedQueries = maxQueuedQueries;
        return this;
    }

    public boolean isDeterminePartitionCountForWriteEnabled()
    {
        return determinePartitionCountForWriteEnabled;
    }

    public QueryManagerConfig setDeterminePartitionCountForWriteEnabled(boolean determinePartitionCountForWriteEnabled)
    {
        this.determinePartitionCountForWriteEnabled = determinePartitionCountForWriteEnabled;
        return this;
    }

    @Min(1)
    public int getMaxHashPartitionCount()
    {
        return maxHashPartitionCount;
    }

    @LegacyConfig({"query.initial-hash-partitions", "query.hash-partition-count"})
    public QueryManagerConfig setMaxHashPartitionCount(int maxHashPartitionCount)
    {
        this.maxHashPartitionCount = maxHashPartitionCount;
        return this;
    }

    @Min(1)
    public int getMinHashPartitionCount()
    {
        return minHashPartitionCount;
    }

    public QueryManagerConfig setMinHashPartitionCount(int minHashPartitionCount)
    {
        this.minHashPartitionCount = minHashPartitionCount;
        return this;
    }

    @Min(1)
    public int getMinHashPartitionCountForWrite()
    {
        return minHashPartitionCountForWrite;
    }

    public QueryManagerConfig setMinHashPartitionCountForWrite(int minHashPartitionCountForWrite)
    {
        this.minHashPartitionCountForWrite = minHashPartitionCountForWrite;
        return this;
    }

    @Min(1)
    public int getMaxWriterTaskCount()
    {
        return maxWriterTaskCount;
    }

    public QueryManagerConfig setMaxWriterTaskCount(int maxWritersNodesCount)
    {
        this.maxWriterTaskCount = maxWritersNodesCount;
        return this;
    }

    @NotNull
    public Duration getMinQueryExpireAge()
    {
        return minQueryExpireAge;
    }

    @LegacyConfig("query.max-age")
    public QueryManagerConfig setMinQueryExpireAge(Duration minQueryExpireAge)
    {
        this.minQueryExpireAge = minQueryExpireAge;
        return this;
    }

    @Min(0)
    public int getMaxQueryHistory()
    {
        return maxQueryHistory;
    }

    public QueryManagerConfig setMaxQueryHistory(int maxQueryHistory)
    {
        this.maxQueryHistory = maxQueryHistory;
        return this;
    }

    @Min(0)
    @Max(1_000_000_000)
    public int getMaxQueryLength()
    {
        return maxQueryLength;
    }

    public QueryManagerConfig setMaxQueryLength(int maxQueryLength)
    {
        this.maxQueryLength = maxQueryLength;
        return this;
    }

    @Min(1)
    public int getMaxStageCount()
    {
        return maxStageCount;
    }

    public QueryManagerConfig setMaxStageCount(int maxStageCount)
    {
        this.maxStageCount = maxStageCount;
        return this;
    }

    @Min(1)
    public int getStageCountWarningThreshold()
    {
        return stageCountWarningThreshold;
    }

    public QueryManagerConfig setStageCountWarningThreshold(int stageCountWarningThreshold)
    {
        this.stageCountWarningThreshold = stageCountWarningThreshold;
        return this;
    }

    @MinDuration("5s")
    @NotNull
    public Duration getClientTimeout()
    {
        return clientTimeout;
    }

    public QueryManagerConfig setClientTimeout(Duration clientTimeout)
    {
        this.clientTimeout = clientTimeout;
        return this;
    }

    @Min(1)
    public int getQueryManagerExecutorPoolSize()
    {
        return queryManagerExecutorPoolSize;
    }

    public QueryManagerConfig setQueryManagerExecutorPoolSize(int queryManagerExecutorPoolSize)
    {
        this.queryManagerExecutorPoolSize = queryManagerExecutorPoolSize;
        return this;
    }

    @Min(1)
    public int getQueryExecutorPoolSize()
    {
        return queryExecutorPoolSize;
    }

    public QueryManagerConfig setQueryExecutorPoolSize(int queryExecutorPoolSize)
    {
        this.queryExecutorPoolSize = queryExecutorPoolSize;
        return this;
    }

    @Min(1)
    public int getMaxStateMachineCallbackThreads()
    {
        return maxStateMachineCallbackThreads;
    }

    public QueryManagerConfig setMaxStateMachineCallbackThreads(int maxStateMachineCallbackThreads)
    {
        this.maxStateMachineCallbackThreads = maxStateMachineCallbackThreads;
        return this;
    }

    @Min(1)
    public int getMaxSplitManagerCallbackThreads()
    {
        return maxSplitManagerCallbackThreads;
    }

    public QueryManagerConfig setMaxSplitManagerCallbackThreads(int maxSplitManagerCallbackThreads)
    {
        this.maxSplitManagerCallbackThreads = maxSplitManagerCallbackThreads;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getRemoteTaskMaxErrorDuration()
    {
        return remoteTaskMaxErrorDuration;
    }

    public QueryManagerConfig setRemoteTaskMaxErrorDuration(Duration remoteTaskMaxErrorDuration)
    {
        this.remoteTaskMaxErrorDuration = remoteTaskMaxErrorDuration;
        return this;
    }

    @NotNull
    public Duration getQueryMaxRunTime()
    {
        return queryMaxRunTime;
    }

    public QueryManagerConfig setQueryMaxRunTime(Duration queryMaxRunTime)
    {
        this.queryMaxRunTime = queryMaxRunTime;
        return this;
    }

    @NotNull
    public Duration getQueryMaxExecutionTime()
    {
        return queryMaxExecutionTime;
    }

    public QueryManagerConfig setQueryMaxExecutionTime(Duration queryMaxExecutionTime)
    {
        this.queryMaxExecutionTime = queryMaxExecutionTime;
        return this;
    }

    @NotNull
    public Duration getQueryMaxPlanningTime()
    {
        return queryMaxPlanningTime;
    }

    public QueryManagerConfig setQueryMaxPlanningTime(Duration queryMaxPlanningTime)
    {
        this.queryMaxPlanningTime = queryMaxPlanningTime;
        return this;
    }

    @NotNull
    @MinDuration("1ns")
    public Duration getQueryMaxCpuTime()
    {
        return queryMaxCpuTime;
    }

    public QueryManagerConfig setQueryMaxCpuTime(Duration queryMaxCpuTime)
    {
        this.queryMaxCpuTime = queryMaxCpuTime;
        return this;
    }

    @NotNull
    public Optional<DataSize> getQueryMaxScanPhysicalBytes()
    {
        return queryMaxScanPhysicalBytes;
    }

    public QueryManagerConfig setQueryMaxScanPhysicalBytes(DataSize queryMaxScanPhysicalBytes)
    {
        this.queryMaxScanPhysicalBytes = Optional.ofNullable(queryMaxScanPhysicalBytes);
        return this;
    }

    @Min(1)
    public int getQueryReportedRuleStatsLimit()
    {
        return queryReportedRuleStatsLimit;
    }

    public QueryManagerConfig setQueryReportedRuleStatsLimit(int queryReportedRuleStatsLimit)
    {
        this.queryReportedRuleStatsLimit = queryReportedRuleStatsLimit;
        return this;
    }

    @Min(1)
    public int getRemoteTaskMaxCallbackThreads()
    {
        return remoteTaskMaxCallbackThreads;
    }

    public QueryManagerConfig setRemoteTaskMaxCallbackThreads(int remoteTaskMaxCallbackThreads)
    {
        this.remoteTaskMaxCallbackThreads = remoteTaskMaxCallbackThreads;
        return this;
    }

    @NotNull
    public String getQueryExecutionPolicy()
    {
        return queryExecutionPolicy;
    }

    public QueryManagerConfig setQueryExecutionPolicy(String queryExecutionPolicy)
    {
        this.queryExecutionPolicy = queryExecutionPolicy;
        return this;
    }

    @Min(1)
    public int getRequiredWorkers()
    {
        return requiredWorkers;
    }

    public QueryManagerConfig setRequiredWorkers(int requiredWorkers)
    {
        this.requiredWorkers = requiredWorkers;
        return this;
    }

    @NotNull
    public Duration getRequiredWorkersMaxWait()
    {
        return requiredWorkersMaxWait;
    }

    public QueryManagerConfig setRequiredWorkersMaxWait(Duration requiredWorkersMaxWait)
    {
        this.requiredWorkersMaxWait = requiredWorkersMaxWait;
        return this;
    }

    @NotNull
    public RetryPolicy getRetryPolicy()
    {
        return retryPolicy;
    }

    public QueryManagerConfig setRetryPolicy(RetryPolicy retryPolicy)
    {
        this.retryPolicy = retryPolicy;
        return this;
    }

    @Min(0)
    public int getQueryRetryAttempts()
    {
        return queryRetryAttempts;
    }

    @LegacyConfig("retry-attempts")
    public QueryManagerConfig setQueryRetryAttempts(int queryRetryAttempts)
    {
        this.queryRetryAttempts = queryRetryAttempts;
        return this;
    }

    @Min(0)
    @Max(MAX_TASK_RETRY_ATTEMPTS)
    public int getTaskRetryAttemptsPerTask()
    {
        return taskRetryAttemptsPerTask;
    }

    public QueryManagerConfig setTaskRetryAttemptsPerTask(int taskRetryAttemptsPerTask)
    {
        this.taskRetryAttemptsPerTask = taskRetryAttemptsPerTask;
        return this;
    }

    @NotNull
    public Duration getRetryInitialDelay()
    {
        return retryInitialDelay;
    }

    public QueryManagerConfig setRetryInitialDelay(Duration retryInitialDelay)
    {
        this.retryInitialDelay = retryInitialDelay;
        return this;
    }

    @NotNull
    public Duration getRetryMaxDelay()
    {
        return retryMaxDelay;
    }

    public QueryManagerConfig setRetryMaxDelay(Duration retryMaxDelay)
    {
        this.retryMaxDelay = retryMaxDelay;
        return this;
    }

    @NotNull
    public double getRetryDelayScaleFactor()
    {
        return retryDelayScaleFactor;
    }

    public QueryManagerConfig setRetryDelayScaleFactor(double retryDelayScaleFactor)
    {
        checkArgument(retryDelayScaleFactor >= 1.0, "retry-delay-scale-factor must be greater than or equal to 1");
        this.retryDelayScaleFactor = retryDelayScaleFactor;
        return this;
    }

    @Min(1)
    public int getMaxTasksWaitingForExecutionPerQuery()
    {
        return maxTasksWaitingForExecutionPerQuery;
    }

    public QueryManagerConfig setMaxTasksWaitingForExecutionPerQuery(int maxTasksWaitingForExecutionPerQuery)
    {
        this.maxTasksWaitingForExecutionPerQuery = maxTasksWaitingForExecutionPerQuery;
        return this;
    }

    @Min(1)
    public int getMaxTasksWaitingForNodePerStage()
    {
        return maxTasksWaitingForNodePerStage;
    }

    public QueryManagerConfig setMaxTasksWaitingForNodePerStage(int maxTasksWaitingForNodePerStage)
    {
        this.maxTasksWaitingForNodePerStage = maxTasksWaitingForNodePerStage;
        return this;
    }

    public boolean isEnabledAdaptiveTaskRequestSize()
    {
        return enabledAdaptiveTaskRequestSize;
    }

    public QueryManagerConfig setEnabledAdaptiveTaskRequestSize(boolean enabledAdaptiveTaskRequestSize)
    {
        this.enabledAdaptiveTaskRequestSize = enabledAdaptiveTaskRequestSize;
        return this;
    }

    @NotNull
    public DataSize getMaxRemoteTaskRequestSize()
    {
        return maxRemoteTaskRequestSize;
    }

    public QueryManagerConfig setMaxRemoteTaskRequestSize(DataSize maxRemoteTaskRequestSize)
    {
        this.maxRemoteTaskRequestSize = maxRemoteTaskRequestSize;
        return this;
    }

    @NotNull
    public DataSize getRemoteTaskRequestSizeHeadroom()
    {
        return remoteTaskRequestSizeHeadroom;
    }

    public QueryManagerConfig setRemoteTaskRequestSizeHeadroom(DataSize remoteTaskRequestSizeHeadroom)
    {
        this.remoteTaskRequestSizeHeadroom = remoteTaskRequestSizeHeadroom;
        return this;
    }

    @Min(1)
    public int getRemoteTaskGuaranteedSplitPerTask()
    {
        return remoteTaskGuaranteedSplitPerTask;
    }

    public QueryManagerConfig setRemoteTaskGuaranteedSplitPerTask(int remoteTaskGuaranteedSplitPerTask)
    {
        this.remoteTaskGuaranteedSplitPerTask = remoteTaskGuaranteedSplitPerTask;
        return this;
    }

    public int getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod()
    {
        return faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod;
    }

    public QueryManagerConfig setFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod(int faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod)
    {
        this.faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod = faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod;
        return this;
    }

    @Min(1)
    public double getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor()
    {
        return faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor;
    }

    public QueryManagerConfig setFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor(double faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor)
    {
        this.faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor = faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor;
        return this;
    }

    @NotNull
    public DataSize getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin()
    {
        return faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin;
    }

    public QueryManagerConfig setFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin(DataSize faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin)
    {
        this.faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin = faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin;
        return this;
    }

    @NotNull
    public DataSize getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax()
    {
        return faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax;
    }

    public QueryManagerConfig setFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax(DataSize faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax)
    {
        this.faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax = faultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax;
        return this;
    }

    public int getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod()
    {
        return faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod;
    }

    public QueryManagerConfig setFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod(int faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod)
    {
        this.faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod = faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod;
        return this;
    }

    @Min(1)
    public double getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor()
    {
        return faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor;
    }

    public QueryManagerConfig setFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor(double faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor)
    {
        this.faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor = faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor;
        return this;
    }

    @NotNull
    public DataSize getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin()
    {
        return faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin;
    }

    public QueryManagerConfig setFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin(DataSize faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin)
    {
        this.faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin = faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin;
        return this;
    }

    @NotNull
    public DataSize getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax()
    {
        return faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax;
    }

    public QueryManagerConfig setFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax(DataSize faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax)
    {
        this.faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax = faultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax;
        return this;
    }

    @NotNull
    public DataSize getFaultTolerantExecutionHashDistributionComputeTaskTargetSize()
    {
        return faultTolerantExecutionHashDistributionComputeTaskTargetSize;
    }

    public QueryManagerConfig setFaultTolerantExecutionHashDistributionComputeTaskTargetSize(DataSize faultTolerantExecutionHashDistributionComputeTaskTargetSize)
    {
        this.faultTolerantExecutionHashDistributionComputeTaskTargetSize = faultTolerantExecutionHashDistributionComputeTaskTargetSize;
        return this;
    }

    @DecimalMin(value = "0.0", inclusive = true)
    public double getFaultTolerantExecutionHashDistributionComputeTasksToNodesMinRatio()
    {
        return faultTolerantExecutionHashDistributionComputeTasksToNodesMinRatio;
    }

    public QueryManagerConfig setFaultTolerantExecutionHashDistributionComputeTasksToNodesMinRatio(double faultTolerantExecutionHashDistributionComputeTasksToNodesMinRatio)
    {
        this.faultTolerantExecutionHashDistributionComputeTasksToNodesMinRatio = faultTolerantExecutionHashDistributionComputeTasksToNodesMinRatio;
        return this;
    }

    @NotNull
    public DataSize getFaultTolerantExecutionHashDistributionWriteTaskTargetSize()
    {
        return faultTolerantExecutionHashDistributionWriteTaskTargetSize;
    }

    public QueryManagerConfig setFaultTolerantExecutionHashDistributionWriteTaskTargetSize(DataSize faultTolerantExecutionHashDistributionWriteTaskTargetSize)
    {
        this.faultTolerantExecutionHashDistributionWriteTaskTargetSize = faultTolerantExecutionHashDistributionWriteTaskTargetSize;
        return this;
    }

    @DecimalMin(value = "0.0", inclusive = true)
    public double getFaultTolerantExecutionHashDistributionWriteTasksToNodesMinRatio()
    {
        return faultTolerantExecutionHashDistributionWriteTasksToNodesMinRatio;
    }

    public QueryManagerConfig setFaultTolerantExecutionHashDistributionWriteTasksToNodesMinRatio(double faultTolerantExecutionHashDistributionWriteTasksToNodesMinRatio)
    {
        this.faultTolerantExecutionHashDistributionWriteTasksToNodesMinRatio = faultTolerantExecutionHashDistributionWriteTasksToNodesMinRatio;
        return this;
    }

    @Min(1)
    public int getFaultTolerantExecutionHashDistributionWriteTaskTargetMaxCount()
    {
        return faultTolerantExecutionHashDistributionWriteTaskTargetMaxCount;
    }

    public QueryManagerConfig setFaultTolerantExecutionHashDistributionWriteTaskTargetMaxCount(int faultTolerantExecutionHashDistributionWriteTaskTargetMaxCount)
    {
        this.faultTolerantExecutionHashDistributionWriteTaskTargetMaxCount = faultTolerantExecutionHashDistributionWriteTaskTargetMaxCount;
        return this;
    }

    @MinDataSize("1MB")
    public DataSize getFaultTolerantExecutionStandardSplitSize()
    {
        return faultTolerantExecutionStandardSplitSize;
    }

    public QueryManagerConfig setFaultTolerantExecutionStandardSplitSize(DataSize faultTolerantExecutionStandardSplitSize)
    {
        this.faultTolerantExecutionStandardSplitSize = faultTolerantExecutionStandardSplitSize;
        return this;
    }

    @Min(1)
    public int getFaultTolerantExecutionMaxTaskSplitCount()
    {
        return faultTolerantExecutionMaxTaskSplitCount;
    }

    public QueryManagerConfig setFaultTolerantExecutionMaxTaskSplitCount(int faultTolerantExecutionMaxTaskSplitCount)
    {
        this.faultTolerantExecutionMaxTaskSplitCount = faultTolerantExecutionMaxTaskSplitCount;
        return this;
    }

    @NotNull
    public DataSize getFaultTolerantExecutionTaskDescriptorStorageMaxMemory()
    {
        return faultTolerantExecutionTaskDescriptorStorageMaxMemory;
    }

    public QueryManagerConfig setFaultTolerantExecutionTaskDescriptorStorageMaxMemory(DataSize faultTolerantExecutionTaskDescriptorStorageMaxMemory)
    {
        this.faultTolerantExecutionTaskDescriptorStorageMaxMemory = faultTolerantExecutionTaskDescriptorStorageMaxMemory;
        return this;
    }

    @Min(1)
    @Max(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT_LIMIT)
    public int getFaultTolerantExecutionMaxPartitionCount()
    {
        return faultTolerantExecutionMaxPartitionCount;
    }

    @LegacyConfig("fault-tolerant-execution-partition-count")
    public QueryManagerConfig setFaultTolerantExecutionMaxPartitionCount(int faultTolerantExecutionMaxPartitionCount)
    {
        this.faultTolerantExecutionMaxPartitionCount = faultTolerantExecutionMaxPartitionCount;
        return this;
    }

    @Min(1)
    @Max(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT_LIMIT)
    public int getFaultTolerantExecutionMinPartitionCount()
    {
        return faultTolerantExecutionMinPartitionCount;
    }

    public QueryManagerConfig setFaultTolerantExecutionMinPartitionCount(int faultTolerantExecutionMinPartitionCount)
    {
        this.faultTolerantExecutionMinPartitionCount = faultTolerantExecutionMinPartitionCount;
        return this;
    }

    @Min(1)
    @Max(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT_LIMIT)
    public int getFaultTolerantExecutionMinPartitionCountForWrite()
    {
        return faultTolerantExecutionMinPartitionCountForWrite;
    }

    public QueryManagerConfig setFaultTolerantExecutionMinPartitionCountForWrite(int faultTolerantExecutionMinPartitionCountForWrite)
    {
        this.faultTolerantExecutionMinPartitionCountForWrite = faultTolerantExecutionMinPartitionCountForWrite;
        return this;
    }

    public boolean isFaultTolerantExecutionRuntimeAdaptivePartitioningEnabled()
    {
        return faultTolerantExecutionRuntimeAdaptivePartitioningEnabled;
    }

    public QueryManagerConfig setFaultTolerantExecutionRuntimeAdaptivePartitioningEnabled(boolean faultTolerantExecutionRuntimeAdaptivePartitioningEnabled)
    {
        this.faultTolerantExecutionRuntimeAdaptivePartitioningEnabled = faultTolerantExecutionRuntimeAdaptivePartitioningEnabled;
        return this;
    }

    @Min(1)
    @Max(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT_LIMIT)
    public int getFaultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount()
    {
        return faultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount;
    }

    public QueryManagerConfig setFaultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount(int faultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount)
    {
        this.faultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount = faultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount;
        return this;
    }

    public DataSize getFaultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize()
    {
        return faultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize;
    }

    public QueryManagerConfig setFaultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize(DataSize faultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize)
    {
        this.faultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize = faultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize;
        return this;
    }

    public double getFaultTolerantExecutionMinSourceStageProgress()
    {
        return faultTolerantExecutionMinSourceStageProgress;
    }

    public QueryManagerConfig setFaultTolerantExecutionMinSourceStageProgress(double faultTolerantExecutionMinSourceStageProgress)
    {
        this.faultTolerantExecutionMinSourceStageProgress = faultTolerantExecutionMinSourceStageProgress;
        return this;
    }

    public boolean isFaultTolerantExecutionSmallStageEstimationEnabled()
    {
        return faultTolerantExecutionSmallStageEstimationEnabled;
    }

    public QueryManagerConfig setFaultTolerantExecutionSmallStageEstimationEnabled(boolean faultTolerantExecutionSmallStageEstimationEnabled)
    {
        this.faultTolerantExecutionSmallStageEstimationEnabled = faultTolerantExecutionSmallStageEstimationEnabled;
        return this;
    }

    public DataSize getFaultTolerantExecutionSmallStageEstimationThreshold()
    {
        return faultTolerantExecutionSmallStageEstimationThreshold;
    }

    public QueryManagerConfig setFaultTolerantExecutionSmallStageEstimationThreshold(DataSize faultTolerantExecutionSmallStageEstimationThreshold)
    {
        this.faultTolerantExecutionSmallStageEstimationThreshold = faultTolerantExecutionSmallStageEstimationThreshold;
        return this;
    }

    @DecimalMin("1.0")
    public double getFaultTolerantExecutionSmallStageSourceSizeMultiplier()
    {
        return faultTolerantExecutionSmallStageSourceSizeMultiplier;
    }

    public QueryManagerConfig setFaultTolerantExecutionSmallStageSourceSizeMultiplier(double faultTolerantExecutionSmallStageSourceSizeMultiplier)
    {
        this.faultTolerantExecutionSmallStageSourceSizeMultiplier = faultTolerantExecutionSmallStageSourceSizeMultiplier;
        return this;
    }

    public boolean isFaultTolerantExecutionSmallStageRequireNoMorePartitions()
    {
        return faultTolerantExecutionSmallStageRequireNoMorePartitions;
    }

    public QueryManagerConfig setFaultTolerantExecutionSmallStageRequireNoMorePartitions(boolean faultTolerantExecutionSmallStageRequireNoMorePartitions)
    {
        this.faultTolerantExecutionSmallStageRequireNoMorePartitions = faultTolerantExecutionSmallStageRequireNoMorePartitions;
        return this;
    }

    public boolean isFaultTolerantExecutionStageEstimationForEagerParentEnabled()
    {
        return faultTolerantExecutionStageEstimationForEagerParentEnabled;
    }

    public QueryManagerConfig setFaultTolerantExecutionStageEstimationForEagerParentEnabled(boolean faultTolerantExecutionStageEstimationForEagerParentEnabled)
    {
        this.faultTolerantExecutionStageEstimationForEagerParentEnabled = faultTolerantExecutionStageEstimationForEagerParentEnabled;
        return this;
    }

    public boolean isFaultTolerantExecutionAdaptiveQueryPlanningEnabled()
    {
        return faultTolerantExecutionAdaptiveQueryPlanningEnabled;
    }

    public QueryManagerConfig setFaultTolerantExecutionAdaptiveQueryPlanningEnabled(boolean faultTolerantExecutionSmallStageEstimationEnabled)
    {
        this.faultTolerantExecutionAdaptiveQueryPlanningEnabled = faultTolerantExecutionSmallStageEstimationEnabled;
        return this;
    }

    public void applyFaultTolerantExecutionDefaults()
    {
        remoteTaskMaxErrorDuration = new Duration(1, MINUTES);
    }
}
