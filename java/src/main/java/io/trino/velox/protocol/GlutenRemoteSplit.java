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
import io.trino.execution.TaskId;
import io.trino.spi.protocol.GlutenConnectorSplit;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GlutenRemoteSplit
        implements GlutenConnectorSplit
{
    private final GlutenLocation location;
    private final TaskId remoteSourceTaskId;

    @JsonCreator
    public GlutenRemoteSplit(@JsonProperty("location") GlutenLocation location, @JsonProperty("remoteSourceTaskId") TaskId remoteSourceTaskId)
    {
        this.location = requireNonNull(location, "location is null");
        this.remoteSourceTaskId = requireNonNull(remoteSourceTaskId, "remoteSourceTaskId is null");
    }

    @JsonProperty
    public GlutenLocation getLocation()
    {
        return location;
    }

    @JsonProperty
    public TaskId getRemoteSourceTaskId()
    {
        return remoteSourceTaskId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("location", location)
                .add("remoteSourceTaskId", remoteSourceTaskId)
                .toString();
    }
}
