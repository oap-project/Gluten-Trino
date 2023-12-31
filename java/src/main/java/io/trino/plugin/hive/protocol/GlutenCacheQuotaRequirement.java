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
import io.airlift.units.DataSize;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GlutenCacheQuotaRequirement
{
    private final GlutenCacheQuotaScope cacheQuotaScope;
    private final Optional<DataSize> quota;

    @JsonCreator
    public GlutenCacheQuotaRequirement(
            @JsonProperty("cacheQuotaScope") GlutenCacheQuotaScope cacheQuotaScope,
            @JsonProperty("quota") Optional<DataSize> quota)
    {
        this.cacheQuotaScope = requireNonNull(cacheQuotaScope, "cacheQuotaScope");
        this.quota = quota;
    }

    @JsonProperty
    public GlutenCacheQuotaScope getCacheQuotaScope()
    {
        return cacheQuotaScope;
    }

    @JsonProperty
    public Optional<DataSize> getQuota()
    {
        return quota;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GlutenCacheQuotaRequirement that = (GlutenCacheQuotaRequirement) o;
        return Objects.equals(cacheQuotaScope, that.cacheQuotaScope) && Objects.equals(quota, that.quota);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cacheQuotaScope, quota);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cacheQuotaScope", cacheQuotaScope)
                .add("quota", quota)
                .toString();
    }
}
