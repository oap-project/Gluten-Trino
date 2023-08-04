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
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.protocol.GlutenConnectorTransactionHandle;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class GlutenPartitioningHandle
{
    private final Optional<GlutenConnectorId> connectorId;
    private final Optional<GlutenConnectorTransactionHandle> transactionHandle;
    private final ConnectorPartitioningHandle connectorHandle;

    @JsonCreator
    public GlutenPartitioningHandle(
            @JsonProperty("connectorId") Optional<GlutenConnectorId> connectorId,
            @JsonProperty("transactionHandle") Optional<GlutenConnectorTransactionHandle> transactionHandle,
            @JsonProperty("connectorHandle") ConnectorPartitioningHandle connectorHandle)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        checkArgument(!connectorId.isPresent() || transactionHandle.isPresent(), "transactionHandle is required when connectorId is present");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
    }

    @JsonProperty
    public Optional<GlutenConnectorId> getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public Optional<GlutenConnectorTransactionHandle> getTransactionHandle()
    {
        return transactionHandle;
    }

    @JsonProperty
    public ConnectorPartitioningHandle getConnectorHandle()
    {
        return connectorHandle;
    }
}
