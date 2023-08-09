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

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

public class NativeTaskConfig
{
    // Native Log verbose level, 'trino_bridge=1,TrinoExchangeSource=2' means that will make
    // the verbose log level in trino_bridge be 1, and the verbose log level in TrinoExchangeSource be 2.
    private String logVerboseModules = "";

    private DataSize maxQueryMemoryPerNode = DataSize.of(1, DataSize.Unit.GIGABYTE);
    private DataSize maxNodeMemory = DataSize.of(1, DataSize.Unit.GIGABYTE);
    private boolean useMmapAllocator;
    private boolean useMmapArena;
    private int mmapArenaCapacityRatio = 10;
    private boolean asyncDataCacheEnabled = true;
    private DataSize asyncCacheSsdSize = DataSize.ofBytes(0);
    private DataSize asyncCacheSsdCheckpointSize = DataSize.ofBytes(0);
    private boolean asyncCacheSsdDisableFileCow;
    private String asyncCacheSsdPath = "/tmp/trino_bridge/cache";
    private boolean enableMemoryLeakCheck = true;
    private boolean enableMemoryArbitration;
    private int reservedMemoryPoolCapacityPercentage = 10;
    private long initMemoryPoolCapacity = 120 << 20;
    private long minMemoryPoolTransferCapacity = 32 << 20;

    @NotNull
    public String getLogVerboseModules()
    {
        return logVerboseModules;
    }

    @Config("native.log-verbose-modules")
    public NativeTaskConfig setLogVerboseModules(String logVerboseModules)
    {
        this.logVerboseModules = logVerboseModules;
        return this;
    }

    @NotNull
    public DataSize getMaxQueryMemoryPerNode()
    {
        return maxQueryMemoryPerNode;
    }

    @Config("native.max-memory-per-node")
    public NativeTaskConfig setMaxQueryMemoryPerNode(DataSize maxQueryMemoryPerNode)
    {
        this.maxQueryMemoryPerNode = maxQueryMemoryPerNode;
        return this;
    }

    @NotNull
    public DataSize getMaxNodeMemory()
    {
        return maxNodeMemory;
    }

    @Config("native.max-node-memory")
    public NativeTaskConfig setMaxNodeMemory(DataSize maxNodeMemory)
    {
        this.maxNodeMemory = maxNodeMemory;
        return this;
    }

    @NotNull
    public boolean isUseMmapAllocator()
    {
        return useMmapAllocator;
    }

    @Config("native.use-mmap-allocator")
    public NativeTaskConfig setUseMmapAllocator(boolean useMmapAllocator)
    {
        this.useMmapAllocator = useMmapAllocator;
        return this;
    }

    @NotNull
    public boolean isUseMmapArena()
    {
        return useMmapArena;
    }

    @Config("native.use-mmap-arena")
    public NativeTaskConfig setUseMmapArena(boolean useMmapArena)
    {
        this.useMmapArena = useMmapArena;
        return this;
    }

    @NotNull
    public int getMmapArenaCapacityRatio()
    {
        return mmapArenaCapacityRatio;
    }

    @Config("native.mmap-arena-capacity-ratio")
    public NativeTaskConfig setMmapArenaCapacityRatio(int mmapArenaCapacityRatio)
    {
        this.mmapArenaCapacityRatio = mmapArenaCapacityRatio;
        return this;
    }

    @NotNull
    public boolean isAsyncDataCacheEnabled()
    {
        return asyncDataCacheEnabled;
    }

    @Config("native.async-data-cache-enabled")
    public NativeTaskConfig setAsyncDataCacheEnabled(boolean asyncDataCacheEnabled)
    {
        this.asyncDataCacheEnabled = asyncDataCacheEnabled;
        return this;
    }

    @NotNull
    public DataSize getAsyncCacheSsdSize()
    {
        return asyncCacheSsdSize;
    }

    @Config("native.async-cache-ssd-size")
    public NativeTaskConfig setAsyncCacheSsdSize(DataSize asyncCacheSsdSize)
    {
        this.asyncCacheSsdSize = asyncCacheSsdSize;
        return this;
    }

    @NotNull
    public DataSize getAsyncCacheSsdCheckpointSize()
    {
        return asyncCacheSsdCheckpointSize;
    }

    @Config("native.async-cache-ssd-checkpoint-size")
    public NativeTaskConfig setAsyncCacheSsdCheckpointSize(DataSize asyncCacheSsdCheckpointSize)
    {
        this.asyncCacheSsdCheckpointSize = asyncCacheSsdCheckpointSize;
        return this;
    }

    @NotNull
    public boolean isAsyncCacheSsdDisableFileCow()
    {
        return asyncCacheSsdDisableFileCow;
    }

    @Config("native.async-cache-ssd-disable-file-cow")
    public NativeTaskConfig setAsyncCacheSsdDisableFileCow(boolean asyncCacheSsdDisableFileCow)
    {
        this.asyncCacheSsdDisableFileCow = asyncCacheSsdDisableFileCow;
        return this;
    }

    @NotNull
    public String getAsyncCacheSsdPath()
    {
        return asyncCacheSsdPath;
    }

    @Config("native.async-cache-ssd-path")
    public NativeTaskConfig setAsyncCacheSsdPath(String asyncCacheSsdPath)
    {
        this.asyncCacheSsdPath = asyncCacheSsdPath;
        return this;
    }

    @NotNull
    public boolean isEnableMemoryLeakCheck()
    {
        return enableMemoryLeakCheck;
    }

    @Config("native.enable-memory-leak-check")
    public NativeTaskConfig setEnableMemoryLeakCheck(boolean enableMemoryLeakCheck)
    {
        this.enableMemoryLeakCheck = enableMemoryLeakCheck;
        return this;
    }

    @NotNull
    public boolean isEnableMemoryArbitration()
    {
        return enableMemoryArbitration;
    }

    @Config("native.enable-memory-arbitration")
    public NativeTaskConfig setEnableMemoryArbitration(boolean enableMemoryArbitration)
    {
        this.enableMemoryArbitration = enableMemoryArbitration;
        return this;
    }

    @NotNull
    public int getReservedMemoryPoolCapacityPercentage()
    {
        return reservedMemoryPoolCapacityPercentage;
    }

    @Config("native.reserved-memory-pool-capacity-percentage")
    public NativeTaskConfig setReservedMemoryPoolCapacityPercentage(int reservedMemoryPoolCapacityPercentage)
    {
        this.reservedMemoryPoolCapacityPercentage = reservedMemoryPoolCapacityPercentage;
        return this;
    }

    @NotNull
    public long getInitMemoryPoolCapacity()
    {
        return initMemoryPoolCapacity;
    }

    @Config("native.init-memory-pool-capacity")
    public NativeTaskConfig setInitMemoryPoolCapacity(long initMemoryPoolCapacity)
    {
        this.initMemoryPoolCapacity = initMemoryPoolCapacity;
        return this;
    }

    @NotNull
    public long getMinMemoryPoolTransferCapacity()
    {
        return minMemoryPoolTransferCapacity;
    }

    @Config("native.min-memory-pool-transfer-capacity")
    public NativeTaskConfig setMinMemoryPoolTransferCapacity(long minMemoryPoolTransferCapacity)
    {
        this.minMemoryPoolTransferCapacity = minMemoryPoolTransferCapacity;
        return this;
    }
}
