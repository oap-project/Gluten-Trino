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
import io.trino.plugin.hive.HiveType;
import io.trino.spi.protocol.GlutenColumnHandle;
import io.trino.spi.protocol.GlutenSubfield;
import io.trino.spi.type.TypeSignature;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.protocol.GlutenHiveColumnHandle.ColumnType.AGGREGATED;
import static io.trino.plugin.hive.protocol.GlutenHiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.protocol.GlutenHiveColumnHandle.ColumnType.SYNTHESIZED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class GlutenHiveColumnHandle
        implements GlutenColumnHandle
{
    private final String name;
    private final HiveType hiveType;
    private final TypeSignature typeName;
    private final int hiveColumnIndex;
    private final ColumnType columnType;
    private final Optional<String> comment;
    private final List<GlutenSubfield> requiredSubfields;

    @JsonCreator
    public GlutenHiveColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("typeSignature") TypeSignature typeSignature,
            @JsonProperty("hiveColumnIndex") int hiveColumnIndex,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("requiredSubfields") List<GlutenSubfield> requiredSubfields)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(hiveColumnIndex >= 0 || columnType == PARTITION_KEY || columnType == SYNTHESIZED || columnType == AGGREGATED, format("hiveColumnIndex:%d is negative, columnType:%s", hiveColumnIndex, columnType.toString()));
        this.hiveColumnIndex = hiveColumnIndex;
        this.hiveType = requireNonNull(hiveType, "hiveType is null");
        this.typeName = requireNonNull(typeSignature, "type is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.requiredSubfields = requireNonNull(requiredSubfields, "requiredSubfields is null");
    }
    /*
    TODO: gluten those fields.
    private final Optional<Aggregation> partialAggregation;
     */

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @JsonProperty
    public int getHiveColumnIndex()
    {
        return hiveColumnIndex;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public TypeSignature getTypeSignature()
    {
        return typeName;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public List<GlutenSubfield> getRequiredSubfields()
    {
        return requiredSubfields;
    }

    public enum ColumnType
    {
        PARTITION_KEY,
        REGULAR,
        SYNTHESIZED,
        AGGREGATED,
    }
}
