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
import io.trino.spi.protocol.MockConnectorSplit;

import static java.util.Objects.requireNonNull;

public class MockSplit
{
    private final MockConnectorId connectorId;
    private final MockConnectorSplit connectorSplit;

    @JsonCreator
    public MockSplit(
            @JsonProperty("connectorId") MockConnectorId connectorId,
            @JsonProperty("connectorSplit") MockConnectorSplit connectorSplit)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.connectorSplit = requireNonNull(connectorSplit, "connectorSplit is null");
    }

    @JsonProperty
    public MockConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public MockConnectorSplit getConnectorSplit()
    {
        return connectorSplit;
    }

    public static MockSplit create(Split split)
    {
        MockConnectorId connectorId = new MockConnectorId(split.getCatalogHandle().getCatalogName());
        MockConnectorSplit mockConnectorSplit = split.getConnectorSplit().getProtocol();
        return new MockSplit(connectorId, mockConnectorSplit);
    }
}
