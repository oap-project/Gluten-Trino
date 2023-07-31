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

public final class MockProjectNode
        extends MockPlanNode
{
    private final MockPlanNode source;
    private final MockAssignments assignments;
    private final Locality locality = Locality.LOCAL;

    @JsonCreator
    public MockProjectNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") MockPlanNode source,
            @JsonProperty("assignments") MockAssignments assignments,
            @JsonProperty("locality") Locality locality)
    {
        this(id, source, assignments);
    }

    public MockProjectNode(PlanNodeId id,
            MockPlanNode source,
            MockAssignments assignments)
    {
        super(id);
        this.source = source;
        this.assignments = assignments;
    }

    @JsonProperty
    public MockPlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public MockAssignments getAssignments()
    {
        return assignments;
    }

    @JsonProperty
    public Locality getLocality()
    {
        return locality;
    }

    public enum Locality
    {
        UNKNOWN,
        LOCAL,
        REMOTE,
    }
}
