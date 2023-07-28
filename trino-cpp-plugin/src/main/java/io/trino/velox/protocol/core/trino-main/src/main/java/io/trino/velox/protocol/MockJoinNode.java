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
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MockJoinNode
        extends MockPlanNode
{
    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }

    public enum Type
    {
        INNER("InnerJoin"),
        LEFT("LeftJoin"),
        RIGHT("RightJoin"),
        FULL("FullJoin");

        private final String joinLabel;

        Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public boolean mustPartition()
        {
            // With REPLICATED, the unmatched rows from right-side would be duplicated.
            return this == RIGHT || this == FULL;
        }

        public boolean mustReplicate(List<EquiJoinClause> criteria)
        {
            // There is nothing to partition on
            return criteria.isEmpty() && (this == INNER || this == LEFT);
        }
    }

    private final Type type;
    private final MockPlanNode left;
    private final MockPlanNode right;
    private final List<EquiJoinClause> criteria;
    private final List<MockVariableReferenceExpression> outputVariables;
    private final Optional<MockRowExpression> filter;
    private final Optional<MockVariableReferenceExpression> leftHashVariable;
    private final Optional<MockVariableReferenceExpression> rightHashVariable;
    private final Optional<DistributionType> distributionType;
    private final Map<String, MockVariableReferenceExpression> dynamicFilters;

    @JsonCreator
    public MockJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("left") MockPlanNode left,
            @JsonProperty("right") MockPlanNode right,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("outputVariables") List<MockVariableReferenceExpression> outputVariables,
            @JsonProperty("filter") Optional<MockRowExpression> filter,
            @JsonProperty("leftHashVariable") Optional<MockVariableReferenceExpression> leftHashVariable,
            @JsonProperty("rightHashVariable") Optional<MockVariableReferenceExpression> rightHashVariable,
            @JsonProperty("distributionType") Optional<DistributionType> distributionType,
            @JsonProperty("dynamicFilters") Map<String, MockVariableReferenceExpression> dynamicFilters)
    {
        super(id);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(outputVariables, "outputVariables is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftHashVariable, "leftHashVariable is null");
        requireNonNull(rightHashVariable, "rightHashVariable is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(dynamicFilters, "dynamicFilters is null");

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = ImmutableList.copyOf(criteria);
        this.outputVariables = ImmutableList.copyOf(outputVariables);
        this.filter = filter;
        this.leftHashVariable = leftHashVariable;
        this.rightHashVariable = rightHashVariable;
        this.distributionType = distributionType;
        this.dynamicFilters = ImmutableMap.copyOf(dynamicFilters);
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public MockPlanNode getLeft()
    {
        return left;
    }

    @JsonProperty
    public MockPlanNode getRight()
    {
        return right;
    }

    @JsonProperty
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty
    public List<MockVariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public Optional<MockRowExpression> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public Optional<MockVariableReferenceExpression> getLeftHashVariable()
    {
        return leftHashVariable;
    }

    @JsonProperty
    public Optional<MockVariableReferenceExpression> getRightHashVariable()
    {
        return rightHashVariable;
    }

    @JsonProperty
    public Optional<DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @JsonProperty
    public Map<String, MockVariableReferenceExpression> getDynamicFilters()
    {
        return dynamicFilters;
    }

    public boolean isCrossJoin()
    {
        return criteria.isEmpty() && filter.isEmpty() && type == Type.INNER;
    }

    public static class EquiJoinClause
    {
        private final MockVariableReferenceExpression left;
        private final MockVariableReferenceExpression right;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") MockVariableReferenceExpression left, @JsonProperty("right") MockVariableReferenceExpression right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        @JsonProperty
        public MockVariableReferenceExpression getLeft()
        {
            return left;
        }

        @JsonProperty
        public MockVariableReferenceExpression getRight()
        {
            return right;
        }

        public EquiJoinClause flip()
        {
            return new EquiJoinClause(right, left);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }

            EquiJoinClause other = (EquiJoinClause) obj;

            return Objects.equals(this.left, other.left) &&
                    Objects.equals(this.right, other.right);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(left, right);
        }

        @Override
        public String toString()
        {
            return format("%s = %s", left, right);
        }
    }
}
