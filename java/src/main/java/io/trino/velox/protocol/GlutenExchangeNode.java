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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.util.MoreLists.listOfListsCopy;

public class GlutenExchangeNode
        extends GlutenPlanNode
{
    private final Type type;
    private final Scope scope;
    private final List<GlutenPlanNode> sources;
    private final GlutenPartitioningScheme partitioningScheme;
    // for each source, the list of inputs corresponding to each output
    private final List<List<GlutenVariableReferenceExpression>> inputs;
    private final boolean ensureSourceOrdering;
    private final Optional<GlutenOrderingScheme> orderingScheme;

    @JsonCreator
    public GlutenExchangeNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("scope") Scope scope,
            @JsonProperty("partitioningScheme") GlutenPartitioningScheme partitioningScheme,
            @JsonProperty("sources") List<GlutenPlanNode> sources,
            @JsonProperty("inputs") List<List<GlutenVariableReferenceExpression>> inputs,
            @JsonProperty("ensureSourceOrdering") boolean ensureSourceOrdering,
            @JsonProperty("orderingScheme") Optional<GlutenOrderingScheme> orderingScheme)
    {
        super(id);
        this.type = type;
        this.sources = sources;
        this.scope = scope;
        this.partitioningScheme = partitioningScheme;
        this.inputs = listOfListsCopy(inputs);
        this.ensureSourceOrdering = ensureSourceOrdering;
        orderingScheme.ifPresent(scheme -> checkArgument(ensureSourceOrdering, "if ordering scheme is present the exchange must ensure source ordering"));
        this.orderingScheme = orderingScheme;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Scope getScope()
    {
        return scope;
    }

    @JsonProperty
    public List<GlutenPlanNode> getSources()
    {
        return sources;
    }

    @JsonProperty
    public GlutenPartitioningScheme getPartitioningScheme()
    {
        return partitioningScheme;
    }

    @JsonProperty
    public List<List<GlutenVariableReferenceExpression>> getInputs()
    {
        return inputs;
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

    public enum Type
    {
        GATHER,
        REPARTITION,
        REPLICATE
    }

    public enum Scope
    {
        LOCAL,
        REMOTE
    }
}
