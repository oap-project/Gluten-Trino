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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class GlutenTupleDomain<T>
{
    private final Optional<Map<T, GlutenDomain>> domains;

    private GlutenTupleDomain(Optional<Map<T, GlutenDomain>> domains)
    {
        requireNonNull(domains, "domains is null");

        this.domains = domains.flatMap(map -> {
            if (containsNoneDomain(map)) {
                return Optional.empty();
            }
            return Optional.of(unmodifiableMap(normalizeAndCopy(map)));
        });
    }

    @JsonCreator
    public static <T> GlutenTupleDomain<T> withColumnDomains(Map<T, GlutenDomain> domains)
    {
        requireNonNull(domains, "domains is null");
        return new GlutenTupleDomain<>(Optional.of(domains));
    }

    @JsonProperty
    // Available for Jackson serialization only!
    public Optional<List<ColumnDomain<T>>> getColumnDomains()
    {
        return domains.map(map -> map.entrySet().stream()
                .map(entry -> new ColumnDomain<>(entry.getKey(), entry.getValue()))
                .collect(toList()));
    }

    private static <T> Map<T, GlutenDomain> normalizeAndCopy(Map<T, GlutenDomain> domains)
    {
        return domains.entrySet().stream()
                .collect(toLinkedMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static <T, K, U> Collector<T, ?, Map<K, U>> toLinkedMap(Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper)
    {
        return toMap(
                keyMapper,
                valueMapper,
                (u, v) -> { throw new IllegalStateException(format("Duplicate values for a key: %s and %s", u, v)); },
                LinkedHashMap::new);
    }

    private static <T> boolean containsNoneDomain(Map<T, GlutenDomain> domains)
    {
        return false;
    }

    public static class ColumnDomain<C>
    {
        private final C column;
        private final GlutenDomain domain;

        @JsonCreator
        public ColumnDomain(
                @JsonProperty("column") C column,
                @JsonProperty("domain") GlutenDomain domain)
        {
            this.column = requireNonNull(column, "column is null");
            this.domain = requireNonNull(domain, "domain is null");
        }

        @JsonProperty
        public C getColumn()
        {
            return column;
        }

        @JsonProperty
        public GlutenDomain getDomain()
        {
            return domain;
        }
    }
}
