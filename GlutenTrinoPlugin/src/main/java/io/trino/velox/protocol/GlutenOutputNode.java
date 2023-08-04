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
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;

public class GlutenOutputNode
        extends GlutenPlanNode
{
    private final GlutenPlanNode source;
    private final List<String> columnNames;
    private final List<GlutenVariableReferenceExpression> outputVariables; // column name = variable.name

    @JsonCreator
    public GlutenOutputNode(@JsonProperty PlanNodeId id, @JsonProperty GlutenPlanNode source, @JsonProperty List<String> columnNames, @JsonProperty List<GlutenVariableReferenceExpression> outputVariables)
    {
        super(id);
        this.source = source;
        this.columnNames = columnNames;
        this.outputVariables = outputVariables;
    }

    public List<GlutenPlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public List<GlutenVariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public GlutenPlanNode getSource()
    {
        return source;
    }
}
