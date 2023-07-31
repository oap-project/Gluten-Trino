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

import javax.validation.constraints.NotNull;

public class NativeTaskConfig
{
    // Native Log verbose level, 'trino_bridge=1,TrinoExchangeSource=2' means that will make
    // the verbose log level in trino_bridge be 1, and the verbose log level in TrinoExchangeSource be 2.
    private String logVerboseModules = "";

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
}
