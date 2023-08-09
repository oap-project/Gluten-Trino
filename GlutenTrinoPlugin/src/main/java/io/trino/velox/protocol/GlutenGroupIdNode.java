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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.util.MoreLists.listOfListsCopy;
import static java.util.Objects.requireNonNull;

public class GlutenGroupIdNode
        extends GlutenPlanNode
{
    private final GlutenPlanNode source;

    // in terms of output variables
    private final List<List<GlutenVariableReferenceExpression>> groupingSets;

    // tracks how each grouping set column is derived from an input column
    private final Map<GlutenVariableReferenceExpression, GlutenVariableReferenceExpression> groupingColumns;
    private final List<GlutenVariableReferenceExpression> aggregationArguments;

    private final GlutenVariableReferenceExpression groupIdVariable;

    @JsonCreator
    public GlutenGroupIdNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") GlutenPlanNode source,
            @JsonProperty("groupingSets") List<List<GlutenVariableReferenceExpression>> groupingSets,
            @JsonProperty("groupingColumns") Map<GlutenVariableReferenceExpression, GlutenVariableReferenceExpression> groupingColumns,
            @JsonProperty("aggregationArguments") List<GlutenVariableReferenceExpression> aggregationArguments,
            @JsonProperty("groupIdVariable") GlutenVariableReferenceExpression groupIdVariable)
    {
        super(id);
        this.source = requireNonNull(source);
        this.groupingSets = listOfListsCopy(requireNonNull(groupingSets, "groupingSets is null"));
        this.groupingColumns = ImmutableMap.copyOf(requireNonNull(groupingColumns));
        this.aggregationArguments = ImmutableList.copyOf(aggregationArguments);
        this.groupIdVariable = requireNonNull(groupIdVariable);

        checkArgument(Sets.intersection(groupingColumns.keySet(), ImmutableSet.copyOf(aggregationArguments)).isEmpty(), "aggregation columns and grouping set columns must be a disjoint set");
    }

    @JsonProperty
    public GlutenPlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<List<GlutenVariableReferenceExpression>> getGroupingSets()
    {
        return groupingSets;
    }

    @JsonProperty
    public Map<GlutenVariableReferenceExpression, GlutenVariableReferenceExpression> getGroupingColumns()
    {
        return groupingColumns;
    }

    @JsonProperty
    public List<GlutenVariableReferenceExpression> getAggregationArguments()
    {
        return aggregationArguments;
    }

    @JsonProperty
    public GlutenVariableReferenceExpression getGroupIdVariable()
    {
        return groupIdVariable;
    }

    // returns the common grouping columns in terms of output symbols
    public Set<GlutenVariableReferenceExpression> getCommonGroupingColumns()
    {
        Set<GlutenVariableReferenceExpression> intersection = new HashSet<>(groupingSets.get(0));
        for (int i = 1; i < groupingSets.size(); i++) {
            intersection.retainAll(groupingSets.get(i));
        }
        return ImmutableSet.copyOf(intersection);
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
        GlutenGroupIdNode that = (GlutenGroupIdNode) o;
        return Objects.equals(source, that.source) &&
                Objects.equals(groupingSets, that.groupingSets) &&
                Objects.equals(groupingColumns, that.groupingColumns) &&
                Objects.equals(aggregationArguments, that.aggregationArguments) &&
                Objects.equals(groupIdVariable, that.groupIdVariable);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, groupingSets, groupingColumns, aggregationArguments, groupIdVariable);
    }
}
