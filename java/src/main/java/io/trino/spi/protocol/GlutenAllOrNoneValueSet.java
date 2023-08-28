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
import io.trino.spi.predicate.AllOrNone;
import io.trino.spi.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class GlutenAllOrNoneValueSet
        implements GlutenValueSet
{
    private final Type type;
    private final boolean all;

    @JsonCreator
    public GlutenAllOrNoneValueSet(@JsonProperty("type") Type type, @JsonProperty("all") boolean all)
    {
        this.type = requireNonNull(type, "type is null");
        this.all = all;
    }

    static GlutenAllOrNoneValueSet all(Type type)
    {
        return new GlutenAllOrNoneValueSet(type, true);
    }

    static GlutenAllOrNoneValueSet none(Type type)
    {
        return new GlutenAllOrNoneValueSet(type, false);
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    public AllOrNone getAllOrNone()
    {
        return () -> all;
    }

    @Override
    public String toString()
    {
        return "[" + (all ? "ALL" : "NONE") + "]";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, all);
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
        final GlutenAllOrNoneValueSet other = (GlutenAllOrNoneValueSet) obj;
        return Objects.equals(this.type, other.type)
                && this.all == other.all;
    }

    private GlutenAllOrNoneValueSet checkCompatibility(GlutenValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched types: %s vs %s", getType(), other.getType()));
        }
        if (!(other instanceof GlutenAllOrNoneValueSet)) {
            throw new IllegalArgumentException(String.format("ValueSet is not a AllOrNoneValueSet: %s", other.getClass()));
        }
        return (GlutenAllOrNoneValueSet) other;
    }
}
