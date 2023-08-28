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
import io.trino.spi.protocol.GlutenConnectorTableHandle;
import io.trino.spi.protocol.GlutenConnectorTransactionHandle;

public final class GlutenTableHandle
{
    private final GlutenConnectorId connectorId;
    // TODO: Push-downed predicates are hold by ConnectorTableHandle, need to modify cpp code to adapt this.
    private final GlutenConnectorTableHandle connectorHandle;
    private final GlutenConnectorTransactionHandle transaction;

    @JsonCreator
    public GlutenTableHandle(@JsonProperty("connectorId") GlutenConnectorId connectorId,
            @JsonProperty("connectorHandle") GlutenConnectorTableHandle connectorHandle,
            @JsonProperty("transaction") GlutenConnectorTransactionHandle transaction)
    {
        this.connectorId = connectorId;
        this.connectorHandle = connectorHandle;
        this.transaction = transaction;
    }

    @JsonProperty
    public GlutenConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public GlutenConnectorTableHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @JsonProperty
    public GlutenConnectorTransactionHandle getTransaction()
    {
        return transaction;
    }
}
