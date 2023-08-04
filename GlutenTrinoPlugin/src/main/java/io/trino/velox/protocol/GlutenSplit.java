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
import io.trino.metadata.Split;
import io.trino.spi.protocol.GlutenConnectorSplit;

import static java.util.Objects.requireNonNull;

public class GlutenSplit
{
    private final GlutenConnectorId connectorId;
    private final GlutenConnectorSplit connectorSplit;

    @JsonCreator
    public GlutenSplit(
            @JsonProperty("connectorId") GlutenConnectorId connectorId,
            @JsonProperty("connectorSplit") GlutenConnectorSplit connectorSplit)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.connectorSplit = requireNonNull(connectorSplit, "connectorSplit is null");
    }

    public static GlutenSplit create(Split split)
    {
        GlutenConnectorId connectorId = new GlutenConnectorId(split.getCatalogHandle().getCatalogName());
        GlutenConnectorSplit glutenConnectorSplit = split.getConnectorSplit().getProtocol();
        return new GlutenSplit(connectorId, glutenConnectorSplit);
    }

    @JsonProperty
    public GlutenConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public GlutenConnectorSplit getConnectorSplit()
    {
        return connectorSplit;
    }
}
