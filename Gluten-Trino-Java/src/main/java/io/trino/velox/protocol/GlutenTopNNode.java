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

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.util.Failures.checkCondition;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class GlutenTopNNode
        extends GlutenPlanNode
{
    private final GlutenPlanNode source;
    private final long count;
    private final GlutenOrderingScheme orderingScheme;
    private final Step step;

    @JsonCreator
    public GlutenTopNNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") GlutenPlanNode source,
            @JsonProperty("count") long count,
            @JsonProperty("orderingScheme") GlutenOrderingScheme orderingScheme,
            @JsonProperty("step") Step step)
    {
        super(id);

        requireNonNull(source, "source is null");
        checkArgument(count >= 0, "count must be positive");
        checkCondition(count <= Integer.MAX_VALUE, NOT_SUPPORTED, "ORDER BY LIMIT > %s is not supported", Integer.MAX_VALUE);
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.source = source;
        this.count = count;
        this.orderingScheme = orderingScheme;
        this.step = requireNonNull(step, "step is null");
    }

    public List<GlutenPlanNode> getSources()
    {
        return singletonList(source);
    }

    @JsonProperty("source")
    public GlutenPlanNode getSource()
    {
        return source;
    }

    @JsonProperty("count")
    public long getCount()
    {
        return count;
    }

    @JsonProperty("orderingScheme")
    public GlutenOrderingScheme getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty("step")
    public Step getStep()
    {
        return step;
    }

    /**
     * Stages of `TopNNode`:
     * <p>
     * SINGLE:    `TopNNode` is in the logical plan.
     * PARTIAL:   `TopNNode` is in the distributed plan, and generates partial results of `TopN` on local workers.
     * FINAL:     `TopNNode` is in the distributed plan, and finalizes the partial results from `PARTIAL` nodes.
     */
    public enum Step
    {
        SINGLE,
        PARTIAL,
        FINAL
    }
}
