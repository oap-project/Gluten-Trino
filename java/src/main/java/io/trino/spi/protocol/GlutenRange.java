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

import static java.util.Objects.requireNonNull;

public class GlutenRange
{
    private final GlutenMarker low;
    private final GlutenMarker high;

    @JsonCreator
    public GlutenRange(
            @JsonProperty("low") GlutenMarker low,
            @JsonProperty("high") GlutenMarker high)
    {
        this(low, high, () -> {
            if (low.compareTo(high) > 0) {
                throw new IllegalArgumentException("low must be less than or equal to high");
            }
        });
    }

    private GlutenRange(GlutenMarker low, GlutenMarker high, Runnable extraCheck)
    {
        requireNonNull(low, "value is null");
        requireNonNull(high, "value is null");
        if (!low.getType().equals(high.getType())) {
            throw new IllegalArgumentException(String.format("Marker types do not match: %s vs %s", low.getType(), high.getType()));
        }
        if (low.getBound() == GlutenMarker.Bound.BELOW) {
            throw new IllegalArgumentException("low bound must be EXACTLY or ABOVE");
        }
        if (high.getBound() == GlutenMarker.Bound.ABOVE) {
            throw new IllegalArgumentException("high bound must be EXACTLY or BELOW");
        }
        extraCheck.run();
        this.low = low;
        this.high = high;
    }

    @JsonProperty
    public GlutenMarker getLow()
    {
        return low;
    }

    @JsonProperty
    public GlutenMarker getHigh()
    {
        return high;
    }
}
