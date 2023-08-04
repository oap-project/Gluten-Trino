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

import java.util.List;
import java.util.Optional;

public class GlutenPartitioningScheme
{
    private final GlutenPartitioning partitioning;
    private final List<GlutenVariableReferenceExpression> outputLayout;
    // TBD: hashColumn isn't used in presto-cpp
    private final boolean replicateNullsAndAny;
    private final Optional<int[]> bucketToPartition;

    @JsonCreator
    public GlutenPartitioningScheme(@JsonProperty("partitioning") GlutenPartitioning partitioning,
            @JsonProperty("outputLayout") List<GlutenVariableReferenceExpression> outputLayout,
            @JsonProperty("replicateNullsAndAny") boolean replicateNullsAndAny,
            @JsonProperty("bucketToPartition") Optional<int[]> bucketToPartition)
    {
        this.partitioning = partitioning;
        this.outputLayout = outputLayout;
        this.replicateNullsAndAny = replicateNullsAndAny;
        this.bucketToPartition = bucketToPartition;
    }

    @JsonProperty
    public GlutenPartitioning getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public List<GlutenVariableReferenceExpression> getOutputLayout()
    {
        return outputLayout;
    }

    @JsonProperty
    public boolean isReplicateNullsAndAny()
    {
        return replicateNullsAndAny;
    }

    @JsonProperty
    public Optional<int[]> getBucketToPartition()
    {
        return bucketToPartition;
    }
}
