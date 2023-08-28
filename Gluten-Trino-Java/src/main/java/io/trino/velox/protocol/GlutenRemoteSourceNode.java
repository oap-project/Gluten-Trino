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
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GlutenRemoteSourceNode
        extends GlutenPlanNode
{
    private final List<PlanFragmentId> sourceFragmentIds;
    private final List<GlutenVariableReferenceExpression> outputVariables;
    private final boolean ensureSourceOrdering;
    private final Optional<GlutenOrderingScheme> orderingScheme;
    private final GlutenExchangeNode.Type exchangeType; // This is needed to "unfragment" to compute stats correctly.

    @JsonCreator
    public GlutenRemoteSourceNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("sourceFragmentIds") List<PlanFragmentId> sourceFragmentIds,
            @JsonProperty("outputVariables") List<GlutenVariableReferenceExpression> outputVariables,
            @JsonProperty("ensureSourceOrdering") boolean ensureSourceOrdering,
            @JsonProperty("orderingScheme") Optional<GlutenOrderingScheme> orderingScheme,
            @JsonProperty("exchangeType") GlutenExchangeNode.Type exchangeType)
    {
        super(id);
        this.sourceFragmentIds = sourceFragmentIds;
        this.outputVariables = ImmutableList.copyOf(requireNonNull(outputVariables, "outputVariables is null"));
        this.ensureSourceOrdering = ensureSourceOrdering;
        this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
        this.exchangeType = requireNonNull(exchangeType, "exchangeType is null");
    }

    @JsonProperty
    public List<PlanFragmentId> getSourceFragmentIds()
    {
        return sourceFragmentIds;
    }

    @JsonProperty
    public List<GlutenVariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public boolean isEnsureSourceOrdering()
    {
        return ensureSourceOrdering;
    }

    @JsonProperty
    public Optional<GlutenOrderingScheme> getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty
    public GlutenExchangeNode.Type getExchangeType()
    {
        return exchangeType;
    }
}
