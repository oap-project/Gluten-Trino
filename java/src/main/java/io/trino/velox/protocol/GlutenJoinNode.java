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

public class GlutenJoinNode
        extends GlutenPlanNode
{
    private final Type type;
    private final GlutenPlanNode left;
    private final GlutenPlanNode right;
    private final List<EquiJoinClause> criteria;
    private final List<GlutenVariableReferenceExpression> outputVariables;
    private final Optional<GlutenRowExpression> filter;
    private final Optional<GlutenVariableReferenceExpression> leftHashVariable;
    private final Optional<GlutenVariableReferenceExpression> rightHashVariable;
    private final Optional<DistributionType> distributionType;
    private final Map<String, GlutenVariableReferenceExpression> dynamicFilters;

    @JsonCreator
    public GlutenJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("left") GlutenPlanNode left,
            @JsonProperty("right") GlutenPlanNode right,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("outputVariables") List<GlutenVariableReferenceExpression> outputVariables,
            @JsonProperty("filter") Optional<GlutenRowExpression> filter,
            @JsonProperty("leftHashVariable") Optional<GlutenVariableReferenceExpression> leftHashVariable,
            @JsonProperty("rightHashVariable") Optional<GlutenVariableReferenceExpression> rightHashVariable,
            @JsonProperty("distributionType") Optional<DistributionType> distributionType,
            @JsonProperty("dynamicFilters") Map<String, GlutenVariableReferenceExpression> dynamicFilters)
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
    public GlutenPlanNode getLeft()
    {
        return left;
    }

    @JsonProperty
    public GlutenPlanNode getRight()
    {
        return right;
    }

    @JsonProperty
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty
    public List<GlutenVariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public Optional<GlutenRowExpression> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public Optional<GlutenVariableReferenceExpression> getLeftHashVariable()
    {
        return leftHashVariable;
    }

    @JsonProperty
    public Optional<GlutenVariableReferenceExpression> getRightHashVariable()
    {
        return rightHashVariable;
    }

    @JsonProperty
    public Optional<DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @JsonProperty
    public Map<String, GlutenVariableReferenceExpression> getDynamicFilters()
    {
        return dynamicFilters;
    }

    public boolean isCrossJoin()
    {
        return criteria.isEmpty() && filter.isEmpty() && type == Type.INNER;
    }

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

    public static class EquiJoinClause
    {
        private final GlutenVariableReferenceExpression left;
        private final GlutenVariableReferenceExpression right;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") GlutenVariableReferenceExpression left, @JsonProperty("right") GlutenVariableReferenceExpression right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        @JsonProperty
        public GlutenVariableReferenceExpression getLeft()
        {
            return left;
        }

        @JsonProperty
        public GlutenVariableReferenceExpression getRight()
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
