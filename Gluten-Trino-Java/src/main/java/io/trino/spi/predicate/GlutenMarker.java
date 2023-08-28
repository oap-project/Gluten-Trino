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
package io.trino.spi.predicate;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.Utils.TUPLE_DOMAIN_TYPE_OPERATORS;
import static io.trino.spi.predicate.Utils.handleThrowable;
import static java.util.Objects.requireNonNull;

/**
 * A point on the continuous space defined by the specified type.
 * Each point may be just below, exact, or just above the specified value according to the Bound.
 */
public final class GlutenMarker
        implements Comparable<GlutenMarker>
{
    public enum Bound
    {
        BELOW,   // lower than the value, but infinitesimally close to the value
        EXACTLY, // exactly the value
        ABOVE    // higher than the value, but infinitesimally close to the value
    }

    private final Type type;
    private final Optional<Block> valueBlock;
    private final Bound bound;
    private final MethodHandle comparisonOperator;
    private final MethodHandle equalOperator;
    private final MethodHandle hashCodeOperator;

    /**
     * LOWER UNBOUNDED is specified with an empty value and a ABOVE bound
     * UPPER UNBOUNDED is specified with an empty value and a BELOW bound
     */
    @JsonCreator
    public GlutenMarker(
            @JsonProperty("type") Type type,
            @JsonProperty("valueBlock") Optional<Block> valueBlock,
            @JsonProperty("bound") Bound bound)
    {
        requireNonNull(type, "type is null");
        requireNonNull(valueBlock, "valueBlock is null");
        requireNonNull(bound, "bound is null");

        if (!type.isOrderable()) {
            throw new IllegalArgumentException("type must be orderable");
        }
        if (!valueBlock.isPresent() && bound == Bound.EXACTLY) {
            throw new IllegalArgumentException("Can not be equal to unbounded");
        }
        if (valueBlock.isPresent() && valueBlock.get().getPositionCount() != 1) {
            throw new IllegalArgumentException("value block should only have one position");
        }
        this.type = type;
        this.valueBlock = valueBlock;
        this.bound = bound;
        this.comparisonOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getComparisonUnorderedLastOperator(type,
                InvocationConvention.simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
        this.equalOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
        this.hashCodeOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
    }

    private static GlutenMarker create(Type type, Optional<Object> value, Bound bound)
    {
        return new GlutenMarker(type, value.map(object -> Utils.nativeValueToBlock(type, object)), bound);
    }

    public static GlutenMarker upperUnbounded(Type type)
    {
        requireNonNull(type, "type is null");
        return create(type, Optional.empty(), Bound.BELOW);
    }

    public static GlutenMarker lowerUnbounded(Type type)
    {
        requireNonNull(type, "type is null");
        return create(type, Optional.empty(), Bound.ABOVE);
    }

    public static GlutenMarker above(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(type, Optional.of(value), Bound.ABOVE);
    }

    public static GlutenMarker exactly(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(type, Optional.of(value), Bound.EXACTLY);
    }

    public static GlutenMarker below(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(type, Optional.of(value), Bound.BELOW);
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<Block> getValueBlock()
    {
        return valueBlock;
    }

    public Object getValue()
    {
        if (!valueBlock.isPresent()) {
            throw new IllegalStateException("No value to get");
        }
        return Utils.blockToNativeValue(type, valueBlock.get());
    }

    @JsonProperty
    public Bound getBound()
    {
        return bound;
    }

    public boolean isUpperUnbounded()
    {
        return !valueBlock.isPresent() && bound == Bound.BELOW;
    }

    public boolean isLowerUnbounded()
    {
        return !valueBlock.isPresent() && bound == Bound.ABOVE;
    }

    private void checkTypeCompatibility(GlutenMarker marker)
    {
        if (!type.equals(marker.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched Marker types: %s vs %s", type, marker.getType()));
        }
    }

    public GlutenMarker greaterAdjacent()
    {
        if (!valueBlock.isPresent()) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                return new GlutenMarker(type, valueBlock, Bound.EXACTLY);
            case EXACTLY:
                return new GlutenMarker(type, valueBlock, Bound.ABOVE);
            case ABOVE:
                throw new IllegalStateException("No greater marker adjacent to an ABOVE bound");
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    public GlutenMarker lesserAdjacent()
    {
        if (!valueBlock.isPresent()) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                throw new IllegalStateException("No lesser marker adjacent to a BELOW bound");
            case EXACTLY:
                return new GlutenMarker(type, valueBlock, Bound.BELOW);
            case ABOVE:
                return new GlutenMarker(type, valueBlock, Bound.EXACTLY);
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    @Override
    public int compareTo(GlutenMarker o)
    {
        checkTypeCompatibility(o);
        if (isUpperUnbounded()) {
            return o.isUpperUnbounded() ? 0 : 1;
        }
        if (isLowerUnbounded()) {
            return o.isLowerUnbounded() ? 0 : -1;
        }
        if (o.isUpperUnbounded()) {
            return -1;
        }
        if (o.isLowerUnbounded()) {
            return 1;
        }
        // INVARIANT: value and o.value are present

        int compare = compare(valueBlock.get(), o.valueBlock.get());
        if (compare == 0) {
            if (bound == o.bound) {
                return 0;
            }
            if (bound == Bound.BELOW) {
                return -1;
            }
            if (bound == Bound.ABOVE) {
                return 1;
            }
            // INVARIANT: bound == EXACTLY
            return (o.bound == Bound.BELOW) ? 1 : -1;
        }
        return compare;
    }

    private int compare(Block left, Block right)
    {
        try {
            return (int) (long) comparisonOperator.invokeExact(left, 0, right, 0);
        }
        catch (RuntimeException | Error e) {
            throw e;
        }
        catch (Throwable throwable) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
        }
    }

    public static GlutenMarker min(GlutenMarker marker1, GlutenMarker marker2)
    {
        return marker1.compareTo(marker2) <= 0 ? marker1 : marker2;
    }

    public static GlutenMarker max(GlutenMarker marker1, GlutenMarker marker2)
    {
        return marker1.compareTo(marker2) >= 0 ? marker1 : marker2;
    }

    @Override
    public int hashCode()
    {
        long hash = Objects.hash(type, bound);
        if (valueBlock.isPresent()) {
            hash = hash * 31 + valueHash();
        }
        return (int) hash;
    }

    private long valueHash()
    {
        try {
            return (long) hashCodeOperator.invokeExact(valueBlock.get(), 0);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }
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
        GlutenMarker other = (GlutenMarker) obj;
        return Objects.equals(this.type, other.type)
                && this.bound == other.bound
                && ((this.valueBlock.isPresent()) == (other.valueBlock.isPresent()))
                && (this.valueBlock.isEmpty() || valueEqual(this.valueBlock.get(), other.valueBlock.get()));
    }

    private boolean valueEqual(Block leftBlock, Block rightBlock)
    {
        try {
            return Boolean.TRUE.equals((Boolean) equalOperator.invokeExact(leftBlock, 0, rightBlock, 0));
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }
    }

    public GlutenMarker canonicalize(boolean removeConstants)
    {
        if (valueBlock.isPresent() && removeConstants) {
            // For REMOVE_CONSTANTS, we replace this with null
            return new GlutenMarker(type, Optional.of(Utils.nativeValueToBlock(type, null)), bound);
        }
        return this;
    }
}
