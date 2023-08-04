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

public final class GlutenFilterNode
        extends GlutenPlanNode
{
    private final GlutenPlanNode source;
    private final GlutenRowExpression predicate;

    @JsonCreator
    public GlutenFilterNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") GlutenPlanNode source,
            @JsonProperty("predicate") GlutenRowExpression predicate)
    {
        super(id);
        this.source = source;
        this.predicate = predicate;
    }

    @JsonProperty
    public GlutenPlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public GlutenRowExpression getPredicate()
    {
        return predicate;
    }
}
