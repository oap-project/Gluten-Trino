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
package io.trino.velox.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class GlutenSortNode
        extends GlutenPlanNode
{
    private final GlutenPlanNode source;
    private final GlutenOrderingScheme orderingScheme;
    private final boolean isPartial;

    @JsonCreator
    public GlutenSortNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") GlutenPlanNode source,
            @JsonProperty("orderingScheme") GlutenOrderingScheme orderingScheme,
            @JsonProperty("isPartial") boolean isPartial)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.source = source;
        this.orderingScheme = orderingScheme;
        this.isPartial = isPartial;
    }

    public List<GlutenPlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public GlutenPlanNode getSource()
    {
        return source;
    }

    @JsonProperty("orderingScheme")
    public GlutenOrderingScheme getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty("isPartial")
    public boolean isPartial()
    {
        return isPartial;
    }
}
