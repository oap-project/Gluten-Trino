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
import io.trino.spi.protocol.GlutenColumnHandle;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;

public class GlutenTableScanNode
        extends GlutenPlanNode
{
    private final GlutenTableHandle table;
    private final Map<GlutenVariableReferenceExpression, GlutenColumnHandle> assignments;
    private final List<GlutenVariableReferenceExpression> outputVariables;

    @JsonCreator
    public GlutenTableScanNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") GlutenTableHandle table,
            @JsonProperty("assignments") Map<GlutenVariableReferenceExpression, GlutenColumnHandle> assignments,
            @JsonProperty("outputVariables") List<GlutenVariableReferenceExpression> outputVariables)
    {
        super(id);
        this.table = table;
        this.assignments = assignments;
        this.outputVariables = outputVariables;
    }

    @JsonProperty
    public GlutenTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Map<GlutenVariableReferenceExpression, GlutenColumnHandle> getAssignments()
    {
        return assignments;
    }

    @JsonProperty
    public List<GlutenVariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }
}
