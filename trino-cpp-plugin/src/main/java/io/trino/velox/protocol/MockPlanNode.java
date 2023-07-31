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
        @JsonSubTypes.Type(value = MockTableScanNode.class, name = "tablescan"),
        @JsonSubTypes.Type(value = MockProjectNode.class, name = "project"),
        @JsonSubTypes.Type(value = MockFilterNode.class, name = "filter"),
        @JsonSubTypes.Type(value = MockAggregationNode.class, name = "aggregation"),
        @JsonSubTypes.Type(value = MockExchangeNode.class, name = "exchange"),
        @JsonSubTypes.Type(value = MockOutputNode.class, name = "output"),
        @JsonSubTypes.Type(value = MockRemoteSourceNode.class, name = "remotesource"),
        @JsonSubTypes.Type(value = MockTopNNode.class, name = "topn"),
        @JsonSubTypes.Type(value = MockLimitNode.class, name = "limit"),
        @JsonSubTypes.Type(value = MockSortNode.class, name = "sort"),
        @JsonSubTypes.Type(value = MockValuesNode.class, name = "values"),
        @JsonSubTypes.Type(value = MockJoinNode.class, name = "join"),
        @JsonSubTypes.Type(value = MockSemiJoinNode.class, name = "semijoin"),
        @JsonSubTypes.Type(value = MockAssignUniqueId.class, name = "assignuniqueid"),
})
public abstract class MockPlanNode
{
    private final PlanNodeId id;

    protected MockPlanNode(PlanNodeId id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @JsonProperty("id")
    public PlanNodeId getId()
    {
        return id;
    }
}
