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

import com.sun.management.OperatingSystemMXBean;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

import java.lang.management.ManagementFactory;

public class NativeTaskConfig
{
    // Native Log verbose level, 'trino_bridge=1,TrinoExchangeSource=2' means that will make
    // the verbose log level in trino_bridge be 1, and the verbose log level in TrinoExchangeSource be 2.
    private String logVerboseModules = "";

    public static final long AVAILABLE_NONHEAP_MEMORY = ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
            .getTotalMemorySize() - Runtime.getRuntime().maxMemory();
    private DataSize maxQueryMemoryPerNode = DataSize.ofBytes(Math.round(AVAILABLE_NONHEAP_MEMORY * 0.3));

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
}
