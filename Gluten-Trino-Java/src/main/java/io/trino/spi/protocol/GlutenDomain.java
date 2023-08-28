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

import static java.util.Objects.requireNonNull;

public final class GlutenDomain
{
    private final GlutenValueSet values;
    private final boolean nullAllowed;

    private GlutenDomain(GlutenValueSet values, boolean nullAllowed)
    {
        this.values = requireNonNull(values, "values is null");
        this.nullAllowed = nullAllowed;
    }

    @JsonCreator
    public static GlutenDomain create(
            @JsonProperty("values") GlutenValueSet values,
            @JsonProperty("nullAllowed") boolean nullAllowed)
    {
        return new GlutenDomain(values, nullAllowed);
    }

    @JsonProperty
    public GlutenValueSet getValues()
    {
        return values;
    }

    @JsonProperty
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }
}
