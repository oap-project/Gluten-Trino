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
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GlutenSemiJoinNode
        extends GlutenPlanNode
{
    private final GlutenPlanNode source;
    private final GlutenPlanNode filteringSource;
    private final GlutenVariableReferenceExpression sourceJoinVariable;
    private final GlutenVariableReferenceExpression filteringSourceJoinVariable;
    private final GlutenVariableReferenceExpression semiJoinOutput;
    private final Optional<GlutenVariableReferenceExpression> sourceHashVariable;
    private final Optional<GlutenVariableReferenceExpression> filteringSourceHashVariable;
    private final Optional<DistributionType> distributionType;
    private final Map<String, GlutenVariableReferenceExpression> dynamicFilters;

    @JsonCreator
    public GlutenSemiJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") GlutenPlanNode source,
            @JsonProperty("filteringSource") GlutenPlanNode filteringSource,
            @JsonProperty("sourceJoinVariable") GlutenVariableReferenceExpression sourceJoinVariable,
            @JsonProperty("filteringSourceJoinVariable") GlutenVariableReferenceExpression filteringSourceJoinVariable,
            @JsonProperty("semiJoinOutput") GlutenVariableReferenceExpression semiJoinOutput,
            @JsonProperty("sourceHashVariable") Optional<GlutenVariableReferenceExpression> sourceHashVariable,
            @JsonProperty("filteringSourceHashVariable") Optional<GlutenVariableReferenceExpression> filteringSourceHashVariable,
            @JsonProperty("distributionType") Optional<DistributionType> distributionType,
            @JsonProperty("dynamicFilters") Map<String, GlutenVariableReferenceExpression> dynamicFilters)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.filteringSource = requireNonNull(filteringSource, "filteringSource is null");
        this.sourceJoinVariable = requireNonNull(sourceJoinVariable, "sourceJoinVariable is null");
        this.filteringSourceJoinVariable = requireNonNull(filteringSourceJoinVariable, "filteringSourceJoinVariable is null");
        this.semiJoinOutput = requireNonNull(semiJoinOutput, "semiJoinOutput is null");
        this.sourceHashVariable = requireNonNull(sourceHashVariable, "sourceHashVariable is null");
        this.filteringSourceHashVariable = requireNonNull(filteringSourceHashVariable, "filteringSourceHashVariable is null");
        this.distributionType = requireNonNull(distributionType, "distributionType is null");
        this.dynamicFilters = requireNonNull(dynamicFilters, "dynamicFilters is null");
    }

    @JsonProperty
    public GlutenPlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public GlutenPlanNode getFilteringSource()
    {
        return filteringSource;
    }

    @JsonProperty
    public GlutenVariableReferenceExpression getSourceJoinVariable()
    {
        return sourceJoinVariable;
    }

    @JsonProperty
    public GlutenVariableReferenceExpression getFilteringSourceJoinVariable()
    {
        return filteringSourceJoinVariable;
    }

    @JsonProperty
    public GlutenVariableReferenceExpression getSemiJoinOutput()
    {
        return semiJoinOutput;
    }

    @JsonProperty
    public Optional<GlutenVariableReferenceExpression> getSourceHashVariable()
    {
        return sourceHashVariable;
    }

    @JsonProperty
    public Optional<GlutenVariableReferenceExpression> getFilteringSourceHashVariable()
    {
        return filteringSourceHashVariable;
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

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }
}
