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
import io.trino.spi.predicate.GlutenMarker;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

public final class GlutenSortedRangeSet
        implements GlutenValueSet
{
    private final Type type;
    private final NavigableMap<GlutenMarker, GlutenRange> lowIndexedRanges;

    private GlutenSortedRangeSet(@JsonProperty("type") Type type,
            @JsonProperty("ranges") NavigableMap<GlutenMarker, GlutenRange> lowIndexedRanges)
    {
        requireNonNull(type, "type is null");
        requireNonNull(lowIndexedRanges, "lowIndexedRanges is null");

        if (!type.isOrderable()) {
            throw new IllegalArgumentException("Type is not orderable: " + type);
        }
        this.type = type;
        this.lowIndexedRanges = lowIndexedRanges;
    }

    @JsonCreator
    public static GlutenSortedRangeSet copyOf(
            @JsonProperty("type") Type type,
            @JsonProperty("ranges") List<GlutenRange> ranges)
    {
        Collections.sort(ranges, Comparator.comparing(GlutenRange::getLow));

        NavigableMap<GlutenMarker, GlutenRange> result = new TreeMap<>();
        for (GlutenRange range : ranges) {
            result.put(range.getLow(), range);
        }
        return new GlutenSortedRangeSet(type, result);
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty("ranges")
    public List<GlutenRange> getOrderedRanges()
    {
        return new ArrayList<>(lowIndexedRanges.values());
    }

    public int getRangeCount()
    {
        return lowIndexedRanges.size();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowIndexedRanges);
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
        final GlutenSortedRangeSet other = (GlutenSortedRangeSet) obj;
        return Objects.equals(this.lowIndexedRanges, other.lowIndexedRanges);
    }
}
