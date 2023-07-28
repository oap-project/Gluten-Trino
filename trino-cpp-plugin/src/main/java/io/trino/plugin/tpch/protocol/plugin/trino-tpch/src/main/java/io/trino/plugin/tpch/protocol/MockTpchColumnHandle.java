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
import io.trino.spi.protocol.MockColumnHandle;
import io.trino.spi.protocol.MockConnectorTableHandle;
import io.trino.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MockTpchColumnHandle
        implements MockColumnHandle
{
    private final String columnName;
    private final Type type;

    @JsonCreator
    public MockTpchColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("type") Type type)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "tpch:" + columnName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        MockTpchColumnHandle other = (MockTpchColumnHandle) o;
        return Objects.equals(columnName, other.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }

    @Override
    public MockTpchColumnHandle refine(MockConnectorTableHandle connectorTableHandle)
    {
        checkArgument(connectorTableHandle instanceof MockTpchTableHandle,
                "The connectorTableHandle for MockColumnHandle must be MockTpchTableHandle.");
        String tableName = ((MockTpchTableHandle) connectorTableHandle).getTableName();
        String columnNamePrefix = switch (tableName) {
            case "customer" -> "c_";
            case "orders" -> "o_";
            case "lineitem" -> "l_";
            case "part" -> "p_";
            case "partsupp" -> "ps_";
            case "supplier" -> "s_";
            case "nation" -> "n_";
            case "region" -> "r_";
            default -> throw new IllegalArgumentException("Table " + tableName + " doesn't exist in TPC-H.");
        };
        return new MockTpchColumnHandle(columnNamePrefix + columnName, type);
    }
}
