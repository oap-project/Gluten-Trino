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
                nativeTaskConfig.getLogVerboseModules());
    }

    @JsonCreator
    public NativeConfigs(
            @JsonProperty long maxOutputPageBytes,
            @JsonProperty int maxWorkerThreads,
            @JsonProperty int maxDriversPerTask,
            @JsonProperty int taskConcurrency,
            @JsonProperty int exchangeClientThreads,
            @JsonProperty long queryMaxMemoryPerNode,
            @JsonProperty String logVerboseModules)
    {
        this.maxOutputPageBytes = maxOutputPageBytes;
        this.maxWorkerThreads = maxWorkerThreads;
        this.maxDriversPerTask = maxDriversPerTask;
        this.taskConcurrency = taskConcurrency;
        this.exchangeClientThreads = exchangeClientThreads;
        this.queryMaxMemoryPerNode = queryMaxMemoryPerNode;
        this.logVerboseModules = logVerboseModules;
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
}
