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
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class GlutenValuesNode
        extends GlutenPlanNode
{
    private final List<GlutenVariableReferenceExpression> outputVariables;
    private final List<List<GlutenRowExpression>> rows;
    // valuesNodeLabel is to record the original table information if the ValuesNode is converted from a table scan.
    // Only used in query plan print, does not affect execution.
    private final Optional<String> valuesNodeLabel;

    @JsonCreator
    public GlutenValuesNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputVariables") List<GlutenVariableReferenceExpression> outputVariables,
            @JsonProperty("rows") List<List<GlutenRowExpression>> rows,
            @JsonProperty("valuesNodeLabel") Optional<String> valuesNodeLabel)
    {
        super(id);
        this.outputVariables = immutableListCopyOf(outputVariables);
        this.rows = immutableListCopyOf(requireNonNull(rows, "lists is null").stream().map(GlutenValuesNode::immutableListCopyOf).collect(Collectors.toList()));

        for (List<GlutenRowExpression> row : rows) {
            if (!(row.size() == outputVariables.size() || row.isEmpty())) {
                throw new IllegalArgumentException(format("Expected row to have %s values, but row has %s values", outputVariables.size(), row.size()));
            }
        }
        this.valuesNodeLabel = valuesNodeLabel;
    }

    private static <T> List<T> immutableListCopyOf(List<T> list)
    {
        return unmodifiableList(new ArrayList<>(requireNonNull(list, "list is null")));
    }

    public Optional<String> getValuesNodeLabel()
    {
        return valuesNodeLabel;
    }

    @JsonProperty
    public List<List<GlutenRowExpression>> getRows()
    {
        return rows;
    }

    @JsonProperty
    public List<GlutenVariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    public List<GlutenPlanNode> getSources()
    {
        return immutableListCopyOf(emptyList());
    }
}
