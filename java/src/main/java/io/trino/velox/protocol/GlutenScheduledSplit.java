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
import com.google.common.primitives.Longs;
import io.trino.execution.ScheduledSplit;
import io.trino.sql.planner.plan.PlanNodeId;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GlutenScheduledSplit
{
    private final long sequenceId;
    private final PlanNodeId planNodeId;
    private final GlutenSplit split;

    @JsonCreator
    public GlutenScheduledSplit(
            @JsonProperty("sequenceId") long sequenceId,
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("split") GlutenSplit split)
    {
        this.sequenceId = sequenceId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.split = requireNonNull(split, "split is null");
    }

    public static GlutenScheduledSplit create(ScheduledSplit scheduledSplit)
    {
        long sequenceId = scheduledSplit.getSequenceId();
        PlanNodeId planNodeId = scheduledSplit.getPlanNodeId();
        GlutenSplit glutenSplit = GlutenSplit.create(scheduledSplit.getSplit());
        return new GlutenScheduledSplit(sequenceId, planNodeId, glutenSplit);
    }

    @JsonProperty
    public long getSequenceId()
    {
        return sequenceId;
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public GlutenSplit getSplit()
    {
        return split;
    }

    @Override
    public int hashCode()
    {
        return Longs.hashCode(sequenceId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        GlutenScheduledSplit other = (GlutenScheduledSplit) obj;
        return this.sequenceId == other.sequenceId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sequenceId", sequenceId)
                .add("planNodeId", planNodeId)
                .add("split", split)
                .toString();
    }
}
