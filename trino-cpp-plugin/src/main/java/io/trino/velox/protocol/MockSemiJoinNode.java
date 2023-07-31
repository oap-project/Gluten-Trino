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

public class MockSemiJoinNode
        extends MockPlanNode
{
    private final MockPlanNode source;
    private final MockPlanNode filteringSource;
    private final MockVariableReferenceExpression sourceJoinVariable;
    private final MockVariableReferenceExpression filteringSourceJoinVariable;
    private final MockVariableReferenceExpression semiJoinOutput;
    private final Optional<MockVariableReferenceExpression> sourceHashVariable;
    private final Optional<MockVariableReferenceExpression> filteringSourceHashVariable;
    private final Optional<DistributionType> distributionType;
    private final Map<String, MockVariableReferenceExpression> dynamicFilters;

    @JsonCreator
    public MockSemiJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") MockPlanNode source,
            @JsonProperty("filteringSource") MockPlanNode filteringSource,
            @JsonProperty("sourceJoinVariable") MockVariableReferenceExpression sourceJoinVariable,
            @JsonProperty("filteringSourceJoinVariable") MockVariableReferenceExpression filteringSourceJoinVariable,
            @JsonProperty("semiJoinOutput") MockVariableReferenceExpression semiJoinOutput,
            @JsonProperty("sourceHashVariable") Optional<MockVariableReferenceExpression> sourceHashVariable,
            @JsonProperty("filteringSourceHashVariable") Optional<MockVariableReferenceExpression> filteringSourceHashVariable,
            @JsonProperty("distributionType") Optional<DistributionType> distributionType,
            @JsonProperty("dynamicFilters") Map<String, MockVariableReferenceExpression> dynamicFilters)
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
    public MockPlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public MockPlanNode getFilteringSource()
    {
        return filteringSource;
    }

    @JsonProperty
    public MockVariableReferenceExpression getSourceJoinVariable()
    {
        return sourceJoinVariable;
    }

    @JsonProperty
    public MockVariableReferenceExpression getFilteringSourceJoinVariable()
    {
        return filteringSourceJoinVariable;
    }

    @JsonProperty
    public MockVariableReferenceExpression getSemiJoinOutput()
    {
        return semiJoinOutput;
    }

    @JsonProperty
    public Optional<MockVariableReferenceExpression> getSourceHashVariable()
    {
        return sourceHashVariable;
    }

    @JsonProperty
    public Optional<MockVariableReferenceExpression> getFilteringSourceHashVariable()
    {
        return filteringSourceHashVariable;
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

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }
}
