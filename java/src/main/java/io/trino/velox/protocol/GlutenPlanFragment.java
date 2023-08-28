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
import io.trino.sql.planner.plan.PlanFragmentId;

public class GlutenPlanFragment
{
    private final PlanFragmentId id;
    private final GlutenPlanNode root;
    private final GlutenPartitioningScheme partitionScheme;
    // TODO: No StageExecutionDescriptor corresponding concept in trino, to modify cpp conversion code as default.

    @JsonCreator
    public GlutenPlanFragment(@JsonProperty("id") PlanFragmentId id,
            @JsonProperty("root") GlutenPlanNode root,
            @JsonProperty("partitioningScheme") GlutenPartitioningScheme partitionScheme)
    {
        this.id = id;
        this.root = root;
        this.partitionScheme = partitionScheme;
    }

    @JsonProperty
    public PlanFragmentId getId()
    {
        return id;
    }

    @JsonProperty
    public GlutenPlanNode getRoot()
    {
        return root;
    }

    @JsonProperty
    public GlutenPartitioningScheme getPartitionScheme()
    {
        return partitionScheme;
    }
}
