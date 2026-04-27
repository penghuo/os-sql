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
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import io.trino.util.PowerOfTwo;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import static io.trino.util.MachineInfo.getAvailablePhysicalProcessorCount;
import static it.unimi.dsi.fastutil.HashCommon.nextPowerOfTwo;
import static java.lang.Math.clamp;
import static java.math.BigDecimal.TWO;

@DefunctConfig({
        "experimental.big-query-max-task-memory",
        "task.max-memory",
        "task.http-notification-threads",
        "task.info-refresh-max-wait",
        "task.operator-pre-allocated-memory",
        "sink.new-implementation",
        "task.legacy-scheduling-behavior",
        "task.level-absolute-priority"})
public class TaskManagerConfig
{
    private boolean threadPerDriverSchedulerEnabled = true;
    private boolean perOperatorCpuTimerEnabled = true;
    private boolean taskCpuTimerEnabled = true;
    private boolean statisticsCpuTimerEnabled = true;
    private DataSize maxPartialAggregationMemoryUsage = DataSize.of(16, Unit.MEGABYTE);
    private DataSize maxPartialTopNMemory = DataSize.of(16, Unit.MEGABYTE);
    private DataSize maxLocalExchangeBufferSize = DataSize.of(128, Unit.MEGABYTE);
    private DataSize maxIndexMemoryUsage = DataSize.of(64, Unit.MEGABYTE);
    private boolean shareIndexLoading;
    private int maxWorkerThreads = Runtime.getRuntime().availableProcessors() * 2;
    private Integer minDrivers;
    private int initialSplitsPerNode = maxWorkerThreads;
    private int minDriversPerTask = 3;
    private int maxDriversPerTask = Integer.MAX_VALUE;
    private Duration splitConcurrencyAdjustmentInterval = new Duration(100, TimeUnit.MILLISECONDS);

    private DataSize sinkMaxBufferSize = DataSize.of(32, Unit.MEGABYTE);
    private DataSize sinkMaxBroadcastBufferSize = DataSize.of(200, Unit.MEGABYTE);
    private DataSize maxPagePartitioningBufferSize = DataSize.of(32, Unit.MEGABYTE);
    private int pagePartitioningBufferPoolSize = 8;

    private Duration clientTimeout = new Duration(2, TimeUnit.MINUTES);
    private Duration infoMaxAge = new Duration(15, TimeUnit.MINUTES);

    private Duration statusRefreshMaxWait = new Duration(1, TimeUnit.SECONDS);
    private Duration infoUpdateInterval = new Duration(3, TimeUnit.SECONDS);
    private Duration taskTerminationTimeout = new Duration(1, TimeUnit.MINUTES);

    private boolean interruptStuckSplitTasksEnabled = true;
    private Duration interruptStuckSplitTasksWarningThreshold = new Duration(10, TimeUnit.MINUTES);
    private Duration interruptStuckSplitTasksTimeout = new Duration(15, TimeUnit.MINUTES);
    private Duration interruptStuckSplitTasksDetectionInterval = new Duration(2, TimeUnit.MINUTES);

    private boolean scaleWritersEnabled = true;
    private int minWriterCount = 1;
    // Set the value of default max writer count to the number of processors * 2 and cap it to 64. It should be
    // above 1, otherwise it can create a plan with a single gather exchange node on the coordinator due to a single
    // available processor. Whereas, on the worker nodes due to more available processors, the default value could
    // be above 1. Therefore, it can cause error due to config mismatch during execution. Additionally, cap
    // it to 64 in order to avoid small pages produced by local partitioning exchanges.
    private int maxWriterCount = clamp(nextPowerOfTwo(getAvailablePhysicalProcessorCount() * 2), 2, 64);
    // Default value of task concurrency should be above 1, otherwise it can create a plan with a single gather
    // exchange node on the coordinator due to a single available processor. Whereas, on the worker nodes due to
    // more available processors, the default value could be above 1. Therefore, it can cause error due to config
    // mismatch during execution. Additionally, cap it to 32 in order to avoid small pages produced by local
    // partitioning exchanges.
    /**
     * default value is overwritten for fault tolerant execution in {@link #applyFaultTolerantExecutionDefaults()}}
     */
    private int taskConcurrency = clamp(nextPowerOfTwo(getAvailablePhysicalProcessorCount()), 2, 32);
    private int httpResponseThreads = 100;
    private int httpTimeoutThreads = 3;

    private int taskNotificationThreads = 5;
    private int taskYieldThreads = 3;
    private int driverTimeoutThreads = 5;

    private BigDecimal levelTimeMultiplier = TWO;

    public TaskManagerConfig setThreadPerDriverSchedulerEnabled(boolean enabled)
    {
        this.threadPerDriverSchedulerEnabled = enabled;
        return this;
    }

    public boolean isThreadPerDriverSchedulerEnabled()
    {
        return threadPerDriverSchedulerEnabled;
    }

    @MinDuration("1ms")
    @MaxDuration("10s")
    @NotNull
    public Duration getStatusRefreshMaxWait()
    {
        return statusRefreshMaxWait;
    }

    public TaskManagerConfig setStatusRefreshMaxWait(Duration statusRefreshMaxWait)
    {
        this.statusRefreshMaxWait = statusRefreshMaxWait;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("10s")
    @NotNull
    public Duration getInfoUpdateInterval()
    {
        return infoUpdateInterval;
    }

    public TaskManagerConfig setInfoUpdateInterval(Duration infoUpdateInterval)
    {
        this.infoUpdateInterval = infoUpdateInterval;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getTaskTerminationTimeout()
    {
        return taskTerminationTimeout;
    }

    public TaskManagerConfig setTaskTerminationTimeout(Duration taskTerminationTimeout)
    {
        this.taskTerminationTimeout = taskTerminationTimeout;
        return this;
    }

    public boolean isPerOperatorCpuTimerEnabled()
    {
        return perOperatorCpuTimerEnabled;
    }

    @LegacyConfig("task.verbose-stats")
    public TaskManagerConfig setPerOperatorCpuTimerEnabled(boolean perOperatorCpuTimerEnabled)
    {
        this.perOperatorCpuTimerEnabled = perOperatorCpuTimerEnabled;
        return this;
    }

    public boolean isTaskCpuTimerEnabled()
    {
        return taskCpuTimerEnabled;
    }

    public TaskManagerConfig setTaskCpuTimerEnabled(boolean taskCpuTimerEnabled)
    {
        this.taskCpuTimerEnabled = taskCpuTimerEnabled;
        return this;
    }

    public boolean isStatisticsCpuTimerEnabled()
    {
        return statisticsCpuTimerEnabled;
    }

    public TaskManagerConfig setStatisticsCpuTimerEnabled(boolean statisticsCpuTimerEnabled)
    {
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
        return this;
    }

    @NotNull
    public DataSize getMaxPartialAggregationMemoryUsage()
    {
        return maxPartialAggregationMemoryUsage;
    }

    public TaskManagerConfig setMaxPartialAggregationMemoryUsage(DataSize maxPartialAggregationMemoryUsage)
    {
        this.maxPartialAggregationMemoryUsage = maxPartialAggregationMemoryUsage;
        return this;
    }

    @NotNull
    public DataSize getMaxPartialTopNMemory()
    {
        return maxPartialTopNMemory;
    }

    public TaskManagerConfig setMaxPartialTopNMemory(DataSize maxPartialTopNMemory)
    {
        this.maxPartialTopNMemory = maxPartialTopNMemory;
        return this;
    }

    @NotNull
    public DataSize getMaxLocalExchangeBufferSize()
    {
        return maxLocalExchangeBufferSize;
    }

    public TaskManagerConfig setMaxLocalExchangeBufferSize(DataSize size)
    {
        this.maxLocalExchangeBufferSize = size;
        return this;
    }

    @NotNull
    public DataSize getMaxIndexMemoryUsage()
    {
        return maxIndexMemoryUsage;
    }

    public TaskManagerConfig setMaxIndexMemoryUsage(DataSize maxIndexMemoryUsage)
    {
        this.maxIndexMemoryUsage = maxIndexMemoryUsage;
        return this;
    }

    @NotNull
    public boolean isShareIndexLoading()
    {
        return shareIndexLoading;
    }

    public TaskManagerConfig setShareIndexLoading(boolean shareIndexLoading)
    {
        this.shareIndexLoading = shareIndexLoading;
        return this;
    }

    @Min(0)
    public BigDecimal getLevelTimeMultiplier()
    {
        return levelTimeMultiplier;
    }

    public TaskManagerConfig setLevelTimeMultiplier(BigDecimal levelTimeMultiplier)
    {
        this.levelTimeMultiplier = levelTimeMultiplier;
        return this;
    }

    @Min(1)
    public int getMaxWorkerThreads()
    {
        return maxWorkerThreads;
    }

    @LegacyConfig("task.shard.max-threads")
    public TaskManagerConfig setMaxWorkerThreads(String maxWorkerThreads)
    {
        this.maxWorkerThreads = ThreadCountParser.DEFAULT.parse(maxWorkerThreads);
        return this;
    }

    @Min(1)
    public int getInitialSplitsPerNode()
    {
        return initialSplitsPerNode;
    }

    public TaskManagerConfig setInitialSplitsPerNode(int initialSplitsPerNode)
    {
        this.initialSplitsPerNode = initialSplitsPerNode;
        return this;
    }

    @MinDuration("1ms")
    public Duration getSplitConcurrencyAdjustmentInterval()
    {
        return splitConcurrencyAdjustmentInterval;
    }

    public TaskManagerConfig setSplitConcurrencyAdjustmentInterval(Duration splitConcurrencyAdjustmentInterval)
    {
        this.splitConcurrencyAdjustmentInterval = splitConcurrencyAdjustmentInterval;
        return this;
    }

    @Min(1)
    public int getMinDrivers()
    {
        if (minDrivers == null) {
            return 2 * maxWorkerThreads;
        }
        return minDrivers;
    }

    public TaskManagerConfig setMinDrivers(int minDrivers)
    {
        this.minDrivers = minDrivers;
        return this;
    }

    @Min(1)
    public int getMaxDriversPerTask()
    {
        return maxDriversPerTask;
    }

    public TaskManagerConfig setMaxDriversPerTask(int maxDriversPerTask)
    {
        this.maxDriversPerTask = maxDriversPerTask;
        return this;
    }

    @Min(1)
    public int getMinDriversPerTask()
    {
        return minDriversPerTask;
    }

    public TaskManagerConfig setMinDriversPerTask(int minDriversPerTask)
    {
        this.minDriversPerTask = minDriversPerTask;
        return this;
    }

    @NotNull
    public DataSize getSinkMaxBufferSize()
    {
        return sinkMaxBufferSize;
    }

    public TaskManagerConfig setSinkMaxBufferSize(DataSize sinkMaxBufferSize)
    {
        this.sinkMaxBufferSize = sinkMaxBufferSize;
        return this;
    }

    public DataSize getSinkMaxBroadcastBufferSize()
    {
        return sinkMaxBroadcastBufferSize;
    }

    public TaskManagerConfig setSinkMaxBroadcastBufferSize(DataSize sinkMaxBroadcastBufferSize)
    {
        this.sinkMaxBroadcastBufferSize = sinkMaxBroadcastBufferSize;
        return this;
    }

    @NotNull
    public DataSize getMaxPagePartitioningBufferSize()
    {
        return maxPagePartitioningBufferSize;
    }

    public TaskManagerConfig setMaxPagePartitioningBufferSize(DataSize size)
    {
        this.maxPagePartitioningBufferSize = size;
        return this;
    }

    @Min(0)
    public int getPagePartitioningBufferPoolSize()
    {
        return pagePartitioningBufferPoolSize;
    }

    public TaskManagerConfig setPagePartitioningBufferPoolSize(int pagePartitioningBufferPoolSize)
    {
        this.pagePartitioningBufferPoolSize = pagePartitioningBufferPoolSize;
        return this;
    }

    @MinDuration("5s")
    @NotNull
    public Duration getClientTimeout()
    {
        return clientTimeout;
    }

    public TaskManagerConfig setClientTimeout(Duration clientTimeout)
    {
        this.clientTimeout = clientTimeout;
        return this;
    }

    @NotNull
    public Duration getInfoMaxAge()
    {
        return infoMaxAge;
    }

    public TaskManagerConfig setInfoMaxAge(Duration infoMaxAge)
    {
        this.infoMaxAge = infoMaxAge;
        return this;
    }

    public boolean isScaleWritersEnabled()
    {
        return scaleWritersEnabled;
    }

    public TaskManagerConfig setScaleWritersEnabled(boolean scaleWritersEnabled)
    {
        this.scaleWritersEnabled = scaleWritersEnabled;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "task.scale-writers.max-writer-count", replacedBy = "task.max-writer-count")
    public TaskManagerConfig setScaleWritersMaxWriterCount(int scaleWritersMaxWriterCount)
    {
        this.maxWriterCount = scaleWritersMaxWriterCount;
        return this;
    }

    @Min(1)
    public int getMinWriterCount()
    {
        return minWriterCount;
    }

    public TaskManagerConfig setMinWriterCount(int minWriterCount)
    {
        this.minWriterCount = minWriterCount;
        return this;
    }

    @Min(1)
    @PowerOfTwo
    public int getMaxWriterCount()
    {
        return maxWriterCount;
    }

    public TaskManagerConfig setMaxWriterCount(int maxWriterCount)
    {
        this.maxWriterCount = maxWriterCount;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "task.partitioned-writer-count", replacedBy = "task.max-writer-count")
    public TaskManagerConfig setPartitionedWriterCount(int partitionedWriterCount)
    {
        this.maxWriterCount = partitionedWriterCount;
        return this;
    }

    @Min(1)
    @PowerOfTwo
    public int getTaskConcurrency()
    {
        return taskConcurrency;
    }

    public TaskManagerConfig setTaskConcurrency(int taskConcurrency)
    {
        this.taskConcurrency = taskConcurrency;
        return this;
    }

    @Min(1)
    public int getHttpResponseThreads()
    {
        return httpResponseThreads;
    }

    public TaskManagerConfig setHttpResponseThreads(int httpResponseThreads)
    {
        this.httpResponseThreads = httpResponseThreads;
        return this;
    }

    @Min(1)
    public int getHttpTimeoutThreads()
    {
        return httpTimeoutThreads;
    }

    public TaskManagerConfig setHttpTimeoutThreads(int httpTimeoutThreads)
    {
        this.httpTimeoutThreads = httpTimeoutThreads;
        return this;
    }

    @Min(1)
    public int getTaskNotificationThreads()
    {
        return taskNotificationThreads;
    }

    public TaskManagerConfig setTaskNotificationThreads(int taskNotificationThreads)
    {
        this.taskNotificationThreads = taskNotificationThreads;
        return this;
    }

    @Min(1)
    public int getTaskYieldThreads()
    {
        return taskYieldThreads;
    }

    public TaskManagerConfig setTaskYieldThreads(int taskYieldThreads)
    {
        this.taskYieldThreads = taskYieldThreads;
        return this;
    }

    @Min(1)
    public int getDriverTimeoutThreads()
    {
        return driverTimeoutThreads;
    }

    public TaskManagerConfig setDriverTimeoutThreads(int driverTimeoutThreads)
    {
        this.driverTimeoutThreads = driverTimeoutThreads;
        return this;
    }

    public boolean isInterruptStuckSplitTasksEnabled()
    {
        return interruptStuckSplitTasksEnabled;
    }

    public TaskManagerConfig setInterruptStuckSplitTasksEnabled(boolean interruptStuckSplitTasksEnabled)
    {
        this.interruptStuckSplitTasksEnabled = interruptStuckSplitTasksEnabled;
        return this;
    }

    @MinDuration("1m")
    public Duration getInterruptStuckSplitTasksWarningThreshold()
    {
        return interruptStuckSplitTasksWarningThreshold;
    }

    public TaskManagerConfig setInterruptStuckSplitTasksWarningThreshold(Duration interruptStuckSplitTasksWarningThreshold)
    {
        this.interruptStuckSplitTasksWarningThreshold = interruptStuckSplitTasksWarningThreshold;
        return this;
    }

    @MinDuration("3m")
    public Duration getInterruptStuckSplitTasksTimeout()
    {
        return interruptStuckSplitTasksTimeout;
    }

    public TaskManagerConfig setInterruptStuckSplitTasksTimeout(Duration interruptStuckSplitTasksTimeout)
    {
        this.interruptStuckSplitTasksTimeout = interruptStuckSplitTasksTimeout;
        return this;
    }

    @MinDuration("1m")
    public Duration getInterruptStuckSplitTasksDetectionInterval()
    {
        return interruptStuckSplitTasksDetectionInterval;
    }

    public TaskManagerConfig setInterruptStuckSplitTasksDetectionInterval(Duration interruptStuckSplitTasksDetectionInterval)
    {
        this.interruptStuckSplitTasksDetectionInterval = interruptStuckSplitTasksDetectionInterval;
        return this;
    }

    public void applyFaultTolerantExecutionDefaults()
    {
        taskConcurrency = 8;
    }
}
