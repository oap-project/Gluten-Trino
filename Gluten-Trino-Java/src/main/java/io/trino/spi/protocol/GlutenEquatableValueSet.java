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

package io.trino.spi.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Utils;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collector;

import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class GlutenEquatableValueSet
        implements GlutenValueSet
{
    private final Type type;
    private final boolean whiteList;
    private final Set<ValueEntry> entries;

    @JsonCreator
    public GlutenEquatableValueSet(
            @JsonProperty("type") Type type,
            @JsonProperty("whiteList") boolean whiteList,
            @JsonProperty("entries") Set<ValueEntry> entries)
    {
        requireNonNull(type, "type is null");
        requireNonNull(entries, "entries is null");

        if (!type.isComparable()) {
            throw new IllegalArgumentException("Type is not comparable: " + type);
        }
        if (type.isOrderable()) {
            throw new IllegalArgumentException("Use SortedRangeSet instead");
        }
        this.type = type;
        this.whiteList = whiteList;
        this.entries = unmodifiableSet(new LinkedHashSet<>(entries));
    }

    @JsonProperty
    @Override
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean isWhiteList()
    {
        return whiteList;
    }

    @JsonProperty
    public Set<ValueEntry> getEntries()
    {
        return entries;
    }

    public Collection<Object> getValues()
    {
        return unmodifiableCollection(entries.stream()
                .map(ValueEntry::getValue)
                .collect(toList()));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, whiteList, entries);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final GlutenEquatableValueSet other = (GlutenEquatableValueSet) obj;
        return Objects.equals(this.type, other.type)
                && this.whiteList == other.whiteList
                && Objects.equals(this.entries, other.entries);
    }

    private static <T> Collector<T, ?, Set<T>> toLinkedSet()
    {
        return toCollection(LinkedHashSet::new);
    }

    public static class ValueEntry
    {
        private final Type type;
        private final Block block;

        @JsonCreator
        public ValueEntry(
                @JsonProperty("type") Type type,
                @JsonProperty("block") Block block)
        {
            this.type = requireNonNull(type, "type is null");
            this.block = requireNonNull(block, "block is null");

            if (block.getPositionCount() != 1) {
                throw new IllegalArgumentException("Block should only have one position");
            }
        }

        public static ValueEntry create(Type type, Object value)
        {
            return new ValueEntry(type, Utils.nativeValueToBlock(type, value));
        }

        @JsonProperty
        public Type getType()
        {
            return type;
        }

        @JsonProperty
        public Block getBlock()
        {
            return block;
        }

        public Object getValue()
        {
            return Utils.blockToNativeValue(type, block);
        }

        @Override
        public int hashCode()
        {
            return 0;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final ValueEntry other = (ValueEntry) obj;
            return Objects.equals(this.type, other.type);
        }
    }
}
