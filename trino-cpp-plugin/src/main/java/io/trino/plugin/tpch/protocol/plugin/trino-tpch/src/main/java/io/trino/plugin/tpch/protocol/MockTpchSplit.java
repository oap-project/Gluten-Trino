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

package io.trino.plugin.tpch.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.protocol.MockConnectorSplit;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MockTpchSplit
        implements MockConnectorSplit
{
    // todo: provide tableHandle message.
    // private final MockTpchTableHandle tableHandle;
    private final int totalParts;
    private final int partNumber;
    private final List<HostAddress> addresses;
    // todo: provide predicate message.
    // private final TupleDomain<MockColumnHandle> predicate;

    @JsonCreator
    public MockTpchSplit(
            @JsonProperty("partNumber") int partNumber,
            @JsonProperty("totalParts") int totalParts,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        checkState(partNumber >= 0, "partNumber must be >= 0");
        checkState(totalParts >= 1, "totalParts must be >= 1");
        checkState(totalParts > partNumber, "totalParts must be > partNumber");

        this.partNumber = partNumber;
        this.totalParts = totalParts;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @JsonProperty
    public int getTotalParts()
    {
        return totalParts;
    }

    @JsonProperty
    public int getPartNumber()
    {
        return partNumber;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }
}
