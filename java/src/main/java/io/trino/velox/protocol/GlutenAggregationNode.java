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

public final class GlutenAggregationNode
        extends GlutenPlanNode
{
    private final GlutenPlanNode source;
    private final Map<GlutenVariableReferenceExpression, Aggregation> aggregations;
    private final GroupingSetDescriptor groupingSets;
    private final List<GlutenVariableReferenceExpression> preGroupedVariables;
    private final Step step;
    private final Optional<GlutenVariableReferenceExpression> hashVariable;
    private final Optional<GlutenVariableReferenceExpression> groupIdVariable;

    @JsonCreator
    public GlutenAggregationNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") GlutenPlanNode source,
            @JsonProperty("aggregations") Map<GlutenVariableReferenceExpression, Aggregation> aggregations,
            @JsonProperty("groupingSets") GroupingSetDescriptor groupingSets,
            @JsonProperty("preGroupedVariables") List<GlutenVariableReferenceExpression> preGroupedVariables,
            @JsonProperty("step") Step step,
            @JsonProperty("hashVariable") Optional<GlutenVariableReferenceExpression> hashVariable,
            @JsonProperty("groupIdVariable") Optional<GlutenVariableReferenceExpression> groupIdVariable)
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
    public GlutenPlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Map<GlutenVariableReferenceExpression, Aggregation> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty
    public GroupingSetDescriptor getGroupingSets()
    {
        return groupingSets;
    }

    @JsonProperty
    public List<GlutenVariableReferenceExpression> getPreGroupedVariables()
    {
        return preGroupedVariables;
    }

    @JsonProperty
    public Step getStep()
    {
        return step;
    }

    @JsonProperty
    public Optional<GlutenVariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }

    @JsonProperty
    public Optional<GlutenVariableReferenceExpression> getGroupIdVariable()
    {
        return groupIdVariable;
    }

    public static class GroupingSetDescriptor
    {
        private final List<GlutenVariableReferenceExpression> groupingKeys;
        private final int groupingSetCount;
        private final Set<Integer> globalGroupingSets;

        @JsonCreator
        public GroupingSetDescriptor(@JsonProperty("groupingKeys") List<GlutenVariableReferenceExpression> groupingKeys,
                @JsonProperty("groupingSetCount") int groupingSetCount,
                @JsonProperty("globalGroupingSets") Set<Integer> globalGroupingSets)
        {
            this.groupingKeys = groupingKeys;
            this.groupingSetCount = groupingSetCount;
            this.globalGroupingSets = globalGroupingSets;
        }

        @JsonProperty
        public List<GlutenVariableReferenceExpression> getGroupingKeys()
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
        private final GlutenCallExpression call;
        private final Optional<GlutenRowExpression> filter;
        private final Optional<GlutenOrderingScheme> orderingScheme;
        private final boolean isDistinct;
        private final Optional<GlutenVariableReferenceExpression> mask;

        private final GlutenFunctionHandle functionHandle;

        private final List<GlutenRowExpression> arguments;

        @JsonCreator
        public Aggregation(@JsonProperty("call") GlutenCallExpression call,
                @JsonProperty("filter") Optional<GlutenRowExpression> filter,
                @JsonProperty("orderingScheme") Optional<GlutenOrderingScheme> orderingScheme,
                @JsonProperty("isDistinct") boolean isDistinct,
                @JsonProperty("mask") Optional<GlutenVariableReferenceExpression> mask,
                @JsonProperty("functionHandle") GlutenFunctionHandle functionHandle,
                @JsonProperty("arguments") List<GlutenRowExpression> arguments)
        {
            this.call = call;
            this.filter = filter;
            this.orderingScheme = orderingScheme;
            this.isDistinct = isDistinct;
            this.mask = mask;
            this.functionHandle = functionHandle;
            this.arguments = arguments;
        }

        @JsonProperty
        public GlutenCallExpression getCall()
        {
            return call;
        }

        @JsonProperty
        public Optional<GlutenRowExpression> getFilter()
        {
            return filter;
        }

        @JsonProperty
        public Optional<GlutenOrderingScheme> getOrderingScheme()
        {
            return orderingScheme;
        }

        @JsonProperty("isDistinct")
        public boolean isDistinct()
        {
            return isDistinct;
        }

        @JsonProperty
        public Optional<GlutenVariableReferenceExpression> getMask()
        {
            return mask;
        }

        @JsonProperty
        public GlutenFunctionHandle getFunctionHandle()
        {
            return functionHandle;
        }

        @JsonProperty
        public List<GlutenRowExpression> getArguments()
        {
            return arguments;
        }
    }
}
