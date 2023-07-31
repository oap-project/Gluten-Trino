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

package io.trino.plugin.hive.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.TableToPartitionMapping;
import io.trino.plugin.hive.metastore.Column;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class MockTableToPartitionMapping
{
    private final Optional<Map<Integer, Integer>> tableToPartitionColumns;
    private final Map<Integer, Column> partitionSchemaDifference;

    @JsonCreator
    public MockTableToPartitionMapping(
            @JsonProperty("tableToPartitionColumns") Optional<Map<Integer, Integer>> tableToPartitionColumns,
            @JsonProperty("partitionSchemaDifference") Map<Integer, Column> partitionSchemaDifference)
    {
        if (tableToPartitionColumns.map(MockTableToPartitionMapping::isIdentityMapping).orElse(true)) {
            this.tableToPartitionColumns = Optional.empty();
        }
        else {
            this.tableToPartitionColumns = requireNonNull(tableToPartitionColumns, "tableToPartitionColumns is null")
                    .map(ImmutableMap::copyOf);
        }
        this.partitionSchemaDifference = ImmutableMap.copyOf(requireNonNull(partitionSchemaDifference, "partitionSchemaDifference is null"));
    }

    @VisibleForTesting
    static boolean isIdentityMapping(Map<Integer, Integer> map)
    {
        for (int i = 0; i < map.size(); i++) {
            if (!Objects.equals(map.get(i), i)) {
                return false;
            }
        }
        return true;
    }

    public static MockTableToPartitionMapping create(TableToPartitionMapping mapping, List<Column> partitionColumns)
    {
        Optional<Map<Integer, Integer>> tableToPartitionColumns = mapping.getTableToPartitionColumns();
        Map<Integer, Column> partitionSchemaDifference = mapping.getPartitionColumnCoercions().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            Integer id = entry.getKey();
                            if (id == null) {
                                throw new IllegalArgumentException("partitionColumnCoercions must not contain nullable key.");
                            }
                            return partitionColumns.get(id);
                        }));
        return new MockTableToPartitionMapping(tableToPartitionColumns, partitionSchemaDifference);
    }

    @JsonProperty
    public Optional<Map<Integer, Integer>> getTableToPartitionColumns()
    {
        return tableToPartitionColumns;
    }

    @JsonProperty
    public Map<Integer, Column> getPartitionSchemaDifference()
    {
        return partitionSchemaDifference;
    }
}
