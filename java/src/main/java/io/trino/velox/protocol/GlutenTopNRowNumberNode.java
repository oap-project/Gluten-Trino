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
import io.trino.velox.protocol.GlutenWindowNode.Specification;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class GlutenTopNRowNumberNode
        extends GlutenPlanNode
{
    private final GlutenPlanNode source;
    private final Specification specification;
    private final GlutenVariableReferenceExpression rowNumberVariable;
    private final int maxRowCountPerPartition;
    private final boolean partial;
    private final Optional<GlutenVariableReferenceExpression> hashVariable;

    @JsonCreator
    public GlutenTopNRowNumberNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") GlutenPlanNode source,
            @JsonProperty("specification") Specification specification,
            @JsonProperty("rowNumberVariable") GlutenVariableReferenceExpression rowNumberVariable,
            @JsonProperty("maxRowCountPerPartition") int maxRowCountPerPartition,
            @JsonProperty("partial") boolean partial,
            @JsonProperty("hashVariable") Optional<GlutenVariableReferenceExpression> hashVariable)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(specification, "specification is null");
        checkArgument(specification.getOrderingScheme().isPresent(), "specification orderingScheme is absent");
        requireNonNull(rowNumberVariable, "rowNumberVariable is null");
        checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");
        requireNonNull(hashVariable, "hashVariable is null");

        this.source = source;
        this.specification = specification;
        this.rowNumberVariable = rowNumberVariable;
        this.maxRowCountPerPartition = maxRowCountPerPartition;
        this.partial = partial;
        this.hashVariable = hashVariable;
    }

    @JsonProperty
    public GlutenPlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Specification getSpecification()
    {
        return specification;
    }

    @JsonProperty
    public GlutenVariableReferenceExpression getRowNumberVariable()
    {
        return rowNumberVariable;
    }

    @JsonProperty
    public int getMaxRowCountPerPartition()
    {
        return maxRowCountPerPartition;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    @JsonProperty
    public Optional<GlutenVariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }
}
