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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class GlutenMarkDistinctNode
        extends GlutenPlanNode
{
    private final GlutenPlanNode source;
    private final GlutenVariableReferenceExpression markerVariable;

    private final Optional<GlutenVariableReferenceExpression> hashVariable;
    private final List<GlutenVariableReferenceExpression> distinctVariables;

    @JsonCreator
    public GlutenMarkDistinctNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") GlutenPlanNode source,
            @JsonProperty("markerVariable") GlutenVariableReferenceExpression markerVariable,
            @JsonProperty("distinctVariables") List<GlutenVariableReferenceExpression> distinctVariables,
            @JsonProperty("hashVariable") Optional<GlutenVariableReferenceExpression> hashVariable)
    {
        super(id);
        this.source = source;
        this.markerVariable = markerVariable;
        this.hashVariable = requireNonNull(hashVariable, "hashVariable is null");
        requireNonNull(distinctVariables, "distinctVariables is null");
        checkArgument(!distinctVariables.isEmpty(), "distinctVariables cannot be empty");
        this.distinctVariables = unmodifiableList(new ArrayList<>(distinctVariables));
    }

    @JsonProperty
    public GlutenPlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public GlutenVariableReferenceExpression getMarkerVariable()
    {
        return markerVariable;
    }

    @JsonProperty
    public List<GlutenVariableReferenceExpression> getDistinctVariables()
    {
        return distinctVariables;
    }

    @JsonProperty
    public Optional<GlutenVariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }
}
