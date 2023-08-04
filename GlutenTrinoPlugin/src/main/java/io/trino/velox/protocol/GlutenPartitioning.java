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
package io.trino.velox.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class GlutenPartitioning
{
    private final GlutenPartitioningHandle handle;
    private final List<GlutenRowExpression> arguments;

    @JsonCreator
    public GlutenPartitioning(@JsonProperty("handle") GlutenPartitioningHandle handle,
            @JsonProperty("arguments") List<GlutenRowExpression> arguments)
    {
        this.handle = handle;
        this.arguments = arguments;
    }

    @JsonProperty
    public GlutenPartitioningHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    public List<GlutenRowExpression> getArguments()
    {
        return arguments;
    }
}
