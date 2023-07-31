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
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class MockAggregationNode
        extends MockPlanNode
{
    private final MockPlanNode source;
    private final Map<MockVariableReferenceExpression, Aggregation> aggregations;
    private final GroupingSetDescriptor groupingSets;
    private final List<MockVariableReferenceExpression> preGroupedVariables;
    private final Step step;
    private final Optional<MockVariableReferenceExpression> hashVariable;
    private final Optional<MockVariableReferenceExpression> groupIdVariable;

    @JsonCreator
    public MockAggregationNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") MockPlanNode source,
            @JsonProperty("aggregations") Map<MockVariableReferenceExpression, Aggregation> aggregations,
            @JsonProperty("groupingSets") GroupingSetDescriptor groupingSets,
            @JsonProperty("preGroupedVariables") List<MockVariableReferenceExpression> preGroupedVariables,
            @JsonProperty("step") Step step,
            @JsonProperty("hashVariable") Optional<MockVariableReferenceExpression> hashVariable,
            @JsonProperty("groupIdVariable") Optional<MockVariableReferenceExpression> groupIdVariable)
    {
        super(id);
        this.source = source;
        this.aggregations = aggregations;
        this.groupingSets = groupingSets;
        this.preGroupedVariables = preGroupedVariables == null ? ImmutableList.of() : preGroupedVariables;
        this.step = step;
        this.hashVariable = hashVariable;
        this.groupIdVariable = groupIdVariable;
    }

    @JsonProperty
    public MockPlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Map<MockVariableReferenceExpression, Aggregation> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty
    public GroupingSetDescriptor getGroupingSets()
    {
        return groupingSets;
    }

    @JsonProperty
    public List<MockVariableReferenceExpression> getPreGroupedVariables()
    {
        return preGroupedVariables;
    }

    @JsonProperty
    public Step getStep()
    {
        return step;
    }

    @JsonProperty
    public Optional<MockVariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }

    @JsonProperty
    public Optional<MockVariableReferenceExpression> getGroupIdVariable()
    {
        return groupIdVariable;
    }

    public static class GroupingSetDescriptor
    {
        private final List<MockVariableReferenceExpression> groupingKeys;
        private final int groupingSetCount;
        private final Set<Integer> globalGroupingSets;

        @JsonCreator
        public GroupingSetDescriptor(@JsonProperty("groupingKeys") List<MockVariableReferenceExpression> groupingKeys,
                @JsonProperty("groupingSetCount") int groupingSetCount,
                @JsonProperty("globalGroupingSets") Set<Integer> globalGroupingSets)
        {
            this.groupingKeys = groupingKeys;
            this.groupingSetCount = groupingSetCount;
            this.globalGroupingSets = globalGroupingSets;
        }

        @JsonProperty
        public List<MockVariableReferenceExpression> getGroupingKeys()
        {
            return groupingKeys;
        }

        @JsonProperty
        public int getGroupingSetCount()
        {
            return groupingSetCount;
        }

        @JsonProperty
        public Set<Integer> getGlobalGroupingSets()
        {
            return globalGroupingSets;
        }
    }

    public static class Aggregation
    {
        private final MockCallExpression call;
        private final Optional<MockRowExpression> filter;
        private final Optional<MockOrderingScheme> orderingScheme;
        private final boolean isDistinct;
        private final Optional<MockVariableReferenceExpression> mask;

        @JsonCreator
        public Aggregation(@JsonProperty("call") MockCallExpression call,
                @JsonProperty("filter") Optional<MockRowExpression> filter,
                @JsonProperty("orderingScheme") Optional<MockOrderingScheme> orderingScheme,
                @JsonProperty("isDistinct") boolean isDistinct,
                @JsonProperty("mask") Optional<MockVariableReferenceExpression> mask)
        {
            this.call = call;
            this.filter = filter;
            this.orderingScheme = orderingScheme;
            this.isDistinct = isDistinct;
            this.mask = mask;
        }

        @JsonProperty
        public MockCallExpression getCall()
        {
            return call;
        }

        @JsonProperty
        public Optional<MockRowExpression> getFilter()
        {
            return filter;
        }

        @JsonProperty
        public Optional<MockOrderingScheme> getOrderingScheme()
        {
            return orderingScheme;
        }

        @JsonProperty("isDistinct")
        public boolean isDistinct()
        {
            return isDistinct;
        }

        @JsonProperty
        public Optional<MockVariableReferenceExpression> getMask()
        {
            return mask;
        }
    }
}
