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
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class MockLimitNode
        extends MockPlanNode
{
    /**
     * Stages of `LimitNode`:
     *
     * PARTIAL:   `LimitNode` is in the distributed plan and generates partial results on local workers.
     * FINAL:     `LimitNode` is in the distributed plan and finalizes the partial results from `PARTIAL` nodes.
     */
    public enum Step
    {
        PARTIAL,
        FINAL
    }

    private final MockPlanNode source;
    private final long count;
    private final Step step;

    @JsonCreator
    public MockLimitNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") MockPlanNode source,
            @JsonProperty("count") long count,
            @JsonProperty("step") Step step)
    {
        super(id);

        checkArgument(count >= 0, "count must be greater than or equal to zero");

        this.source = requireNonNull(source, "source is null");
        this.count = count;
        this.step = requireNonNull(step, "step is null");
    }

    public List<MockPlanNode> getSources()
    {
        return singletonList(source);
    }

    /**
     * LimitNode only expects a single upstream PlanNode.
     */
    @JsonProperty
    public MockPlanNode getSource()
    {
        return source;
    }

    /**
     * Get the limit `N` number of results to return.
     */
    @JsonProperty
    public long getCount()
    {
        return count;
    }

    @JsonProperty
    public Step getStep()
    {
        return step;
    }

    public boolean isPartial()
    {
        return step == Step.PARTIAL;
    }
}
