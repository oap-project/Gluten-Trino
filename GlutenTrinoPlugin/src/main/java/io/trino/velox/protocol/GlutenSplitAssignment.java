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
import com.google.common.collect.ImmutableSet;
import io.trino.execution.SplitAssignment;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class GlutenSplitAssignment
{
    private final PlanNodeId planNodeId;
    private final Set<GlutenScheduledSplit> splits;
    private final boolean noMoreSplits;

    @JsonCreator
    public GlutenSplitAssignment(
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("splits") Set<GlutenScheduledSplit> splits,
            @JsonProperty("noMoreSplits") boolean noMoreSplits)
    {
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.splits = ImmutableSet.copyOf(requireNonNull(splits, "splits is null"));
        this.noMoreSplits = noMoreSplits;
    }

    public static GlutenSplitAssignment create(SplitAssignment splitAssignment)
    {
        PlanNodeId planNodeId = splitAssignment.getPlanNodeId();
        Set<GlutenScheduledSplit> glutenScheduledSplits = splitAssignment.getSplits().stream()
                .map(GlutenScheduledSplit::create)
                .collect(Collectors.toSet());
        boolean noMoreSplits = splitAssignment.isNoMoreSplits();
        return new GlutenSplitAssignment(planNodeId, glutenScheduledSplits, noMoreSplits);
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public Set<GlutenScheduledSplit> getSplits()
    {
        return splits;
    }

    @JsonProperty
    public boolean isNoMoreSplits()
    {
        return noMoreSplits;
    }
}
