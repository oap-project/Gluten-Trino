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

package io.trino.velox;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.trino.execution.TaskManagerConfig;
import io.trino.operator.DirectExchangeClientConfig;

public class NativeConfigs
{
    private final long maxOutputPageBytes;
    private final int maxWorkerThreads;
    private final int maxDriversPerTask;
    private final int taskConcurrency;
    private final int exchangeClientThreads;
    private final long queryMaxMemoryPerNode;
    private final String logVerboseModules;
    private final long maxNodeMemory;
    private final boolean useMmapAllocator;
    private final boolean useMmapArena;
    private final int mmapArenaCapacityRatio;
    private final boolean asyncDataCacheEnabled;
    private final long asyncCacheSsdSize;
    private final long asyncCacheSsdCheckpointSize;
    private final boolean asyncCacheSsdDisableFileCow;
    private final String asyncCacheSsdPath;
    private final boolean enableMemoryLeakCheck;
    private final boolean enableMemoryArbitration;
    private final int reservedMemoryPoolCapacityPercentage;
    private final long initMemoryPoolCapacity;
    private final long minMemoryPoolTransferCapacity;
    private final int maxHttpSessionReadBufferSize;
    private final boolean joinSpillEnabled;
    private final boolean aggSpillEnabled;
    private final boolean orderBySpillEnabled;
    private final boolean spillEnabled;
    private final String spillDir;
    private final long aggregationSpillMemoryThreshold;
    private final long joinSpillMemoryThreshold;
    private final long orderBySpillMemoryThreshold;

    @Inject
    public NativeConfigs(
            TaskManagerConfig taskManagerConfig,
            DirectExchangeClientConfig directExchangeClientConfig,
            NativeTaskConfig nativeTaskConfig)
    {
        this(directExchangeClientConfig.getMaxResponseSize().toBytes(),
                taskManagerConfig.getMaxWorkerThreads(),
                taskManagerConfig.getMaxDriversPerTask(),
                taskManagerConfig.getTaskConcurrency(),
                directExchangeClientConfig.getClientThreads(),
                nativeTaskConfig.getMaxQueryMemoryPerNode().toBytes(),
                nativeTaskConfig.getLogVerboseModules(),
                nativeTaskConfig.getMaxNodeMemory().toBytes(),
                nativeTaskConfig.isUseMmapAllocator(),
                nativeTaskConfig.isUseMmapArena(),
                nativeTaskConfig.getMmapArenaCapacityRatio(),
                nativeTaskConfig.isAsyncDataCacheEnabled(),
                nativeTaskConfig.getAsyncCacheSsdSize().toBytes(),
                nativeTaskConfig.getAsyncCacheSsdCheckpointSize().toBytes(),
                nativeTaskConfig.isAsyncCacheSsdDisableFileCow(),
                nativeTaskConfig.getAsyncCacheSsdPath(),
                nativeTaskConfig.isEnableMemoryLeakCheck(),
                nativeTaskConfig.isEnableMemoryArbitration(),
                nativeTaskConfig.getReservedMemoryPoolCapacityPercentage(),
                nativeTaskConfig.getInitMemoryPoolCapacity(),
                nativeTaskConfig.getMinMemoryPoolTransferCapacity(),
                nativeTaskConfig.getMaxHttpSessionReadBufferSize(),
                nativeTaskConfig.isSpillEnabled(),
                nativeTaskConfig.isJoinSpillEnabled(),
                nativeTaskConfig.isAggSpillEnabled(),
                nativeTaskConfig.isOrderBySpillEnabled(),
                nativeTaskConfig.getSpillDir(),
                nativeTaskConfig.getJoinSpillMemoryThreshold(),
                nativeTaskConfig.getAggregationSpillMemoryThreshold(),
                nativeTaskConfig.getOrderBySpillMemoryThreshold());
    }

    @JsonCreator
    public NativeConfigs(
            @JsonProperty long maxOutputPageBytes,
            @JsonProperty int maxWorkerThreads,
            @JsonProperty int maxDriversPerTask,
            @JsonProperty int taskConcurrency,
            @JsonProperty int exchangeClientThreads,
            @JsonProperty long queryMaxMemoryPerNode,
            @JsonProperty String logVerboseModules,
            @JsonProperty long maxNodeMemory,
            @JsonProperty boolean useMmapAllocator,
            @JsonProperty boolean useMmapArena,
            @JsonProperty int mmapArenaCapacityRatio,
            @JsonProperty boolean asyncDataCacheEnabled,
            @JsonProperty long asyncCacheSsdSize,
            @JsonProperty long asyncCacheSsdCheckpointSize,
            @JsonProperty boolean asyncCacheSsdDisableFileCow,
            @JsonProperty String asyncCacheSsdPath,
            @JsonProperty boolean enableMemoryLeakCheck,
            @JsonProperty boolean enableMemoryArbitration,
            @JsonProperty int reservedMemoryPoolCapacityPercentage,
            @JsonProperty long initMemoryPoolCapacity,
            @JsonProperty long minMemoryPoolTransferCapacity,
            @JsonProperty int maxHttpSessionReadBufferSize,
            @JsonProperty boolean spillEnabled,
            @JsonProperty boolean joinSpillEnabled,
            @JsonProperty boolean aggSpillEnabled,
            @JsonProperty boolean orderBySpillEnabled,
            @JsonProperty String spillDir,
            @JsonProperty long joinSpillMemoryThreshold,
            @JsonProperty long aggregationSpillMemoryThreshold,
            @JsonProperty long orderBySpillMemoryThreshold)
    {
        this.maxOutputPageBytes = maxOutputPageBytes;
        this.maxWorkerThreads = maxWorkerThreads;
        this.maxDriversPerTask = maxDriversPerTask;
        this.taskConcurrency = taskConcurrency;
        this.exchangeClientThreads = exchangeClientThreads;
        this.queryMaxMemoryPerNode = queryMaxMemoryPerNode;
        this.logVerboseModules = logVerboseModules;
        this.maxNodeMemory = maxNodeMemory;
        this.useMmapAllocator = useMmapAllocator;
        this.useMmapArena = useMmapArena;
        this.mmapArenaCapacityRatio = mmapArenaCapacityRatio;
        this.asyncDataCacheEnabled = asyncDataCacheEnabled;
        this.asyncCacheSsdSize = asyncCacheSsdSize;
        this.asyncCacheSsdCheckpointSize = asyncCacheSsdCheckpointSize;
        this.asyncCacheSsdDisableFileCow = asyncCacheSsdDisableFileCow;
        this.asyncCacheSsdPath = asyncCacheSsdPath;
        this.enableMemoryLeakCheck = enableMemoryLeakCheck;
        this.enableMemoryArbitration = enableMemoryArbitration;
        this.reservedMemoryPoolCapacityPercentage = reservedMemoryPoolCapacityPercentage;
        this.initMemoryPoolCapacity = initMemoryPoolCapacity;
        this.minMemoryPoolTransferCapacity = minMemoryPoolTransferCapacity;
        this.maxHttpSessionReadBufferSize = maxHttpSessionReadBufferSize;
        this.spillEnabled = spillEnabled;
        this.joinSpillEnabled = joinSpillEnabled;
        this.aggSpillEnabled = aggSpillEnabled;
        this.orderBySpillEnabled = orderBySpillEnabled;
        this.spillDir = spillDir;
        this.joinSpillMemoryThreshold = joinSpillMemoryThreshold;
        this.aggregationSpillMemoryThreshold = aggregationSpillMemoryThreshold;
        this.orderBySpillMemoryThreshold = orderBySpillMemoryThreshold;
    }

    @JsonProperty
    public long getMaxOutputPageBytes()
    {
        return maxOutputPageBytes;
    }

    @JsonProperty
    public int getMaxWorkerThreads()
    {
        return maxWorkerThreads;
    }

    @JsonProperty
    public int getMaxDriversPerTask()
    {
        return maxDriversPerTask;
    }

    @JsonProperty
    public int getTaskConcurrency()
    {
        return taskConcurrency;
    }

    @JsonProperty
    public int getExchangeClientThreads()
    {
        return exchangeClientThreads;
    }

    @JsonProperty
    public long getQueryMaxMemoryPerNode()
    {
        return queryMaxMemoryPerNode;
    }

    @JsonProperty
    public String getLogVerboseModules()
    {
        return logVerboseModules;
    }

    @JsonProperty
    public long getMaxNodeMemory()
    {
        return maxNodeMemory;
    }

    @JsonProperty
    public boolean isUseMmapAllocator()
    {
        return useMmapAllocator;
    }

    @JsonProperty
    public boolean isUseMmapArena()
    {
        return useMmapArena;
    }

    @JsonProperty
    public int getMmapArenaCapacityRatio()
    {
        return mmapArenaCapacityRatio;
    }

    @JsonProperty
    public boolean isAsyncDataCacheEnabled()
    {
        return asyncDataCacheEnabled;
    }

    @JsonProperty
    public long getAsyncCacheSsdSize()
    {
        return asyncCacheSsdSize;
    }

    @JsonProperty
    public long getAsyncCacheSsdCheckpointSize()
    {
        return asyncCacheSsdCheckpointSize;
    }

    @JsonProperty
    public boolean isAsyncCacheSsdDisableFileCow()
    {
        return asyncCacheSsdDisableFileCow;
    }

    @JsonProperty
    public String getAsyncCacheSsdPath()
    {
        return asyncCacheSsdPath;
    }

    @JsonProperty
    public boolean isEnableMemoryLeakCheck()
    {
        return enableMemoryLeakCheck;
    }

    @JsonProperty
    public boolean isEnableMemoryArbitration()
    {
        return enableMemoryArbitration;
    }

    @JsonProperty
    public int getReservedMemoryPoolCapacityPercentage()
    {
        return reservedMemoryPoolCapacityPercentage;
    }

    @JsonProperty
    public long getInitMemoryPoolCapacity()
    {
        return initMemoryPoolCapacity;
    }

    @JsonProperty
    public long getMinMemoryPoolTransferCapacity()
    {
        return minMemoryPoolTransferCapacity;
    }

    @JsonProperty
    public int getMaxHttpSessionReadBufferSize()
    {
        return maxHttpSessionReadBufferSize;
    }

    @JsonProperty
    public boolean getSpillEnabled()
    {
        return spillEnabled;
    }

    @JsonProperty
    public boolean getJoinSpillEnabled()
    {
        return joinSpillEnabled;
    }

    @JsonProperty
    public boolean getAggSpillEnabled()
    {
        return aggSpillEnabled;
    }

    @JsonProperty
    public boolean getOrderBySpillEnabled()
    {
        return orderBySpillEnabled;
    }

    @JsonProperty
    public String getSpillDir()
    {
        return spillDir;
    }

    @JsonProperty
    public long getAggregationSpillMemoryThreshold()
    {
        return aggregationSpillMemoryThreshold;
    }

    @JsonProperty
    public long getJoinSpillMemoryThreshold()
    {
        return joinSpillMemoryThreshold;
    }

    @JsonProperty
    public long getOrderBySpillMemoryThreshold()
    {
        return orderBySpillMemoryThreshold;
    }
}
