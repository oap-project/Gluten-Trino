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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.sql.planner.plan.PlanNodeId;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = GlutenTableScanNode.class, name = "tablescan"),
        @JsonSubTypes.Type(value = GlutenProjectNode.class, name = "project"),
        @JsonSubTypes.Type(value = GlutenFilterNode.class, name = "filter"),
        @JsonSubTypes.Type(value = GlutenAggregationNode.class, name = "aggregation"),
        @JsonSubTypes.Type(value = GlutenExchangeNode.class, name = "exchange"),
        @JsonSubTypes.Type(value = GlutenOutputNode.class, name = "output"),
        @JsonSubTypes.Type(value = GlutenRemoteSourceNode.class, name = "remotesource"),
        @JsonSubTypes.Type(value = GlutenTopNNode.class, name = "topn"),
        @JsonSubTypes.Type(value = GlutenLimitNode.class, name = "limit"),
        @JsonSubTypes.Type(value = GlutenSortNode.class, name = "sort"),
        @JsonSubTypes.Type(value = GlutenValuesNode.class, name = "values"),
        @JsonSubTypes.Type(value = GlutenJoinNode.class, name = "join"),
        @JsonSubTypes.Type(value = GlutenSemiJoinNode.class, name = "semijoin"),
        @JsonSubTypes.Type(value = GlutenAssignUniqueId.class, name = "assignuniqueid"),
        @JsonSubTypes.Type(value = GlutenEnforceSingleRowNode.class, name = "enforcesinglerow"),
        @JsonSubTypes.Type(value = GlutenGroupIdNode.class, name = "groupid"),
        @JsonSubTypes.Type(value = GlutenMarkDistinctNode.class, name = "markdistinct"),
        @JsonSubTypes.Type(value = GlutenWindowNode.class, name = "window"),
        @JsonSubTypes.Type(value = GlutenTopNRowNumberNode.class, name = "topnrownumber"),
})
public abstract class GlutenPlanNode
{
    private final PlanNodeId id;

    protected GlutenPlanNode(PlanNodeId id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @JsonProperty("id")
    public PlanNodeId getId()
    {
        return id;
    }
}
