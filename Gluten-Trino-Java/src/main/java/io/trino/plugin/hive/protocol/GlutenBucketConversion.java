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

package io.trino.plugin.hive.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class GlutenBucketConversion
{
    private final int tableBucketCount;
    private final int partitionBucketCount;
    private final List<GlutenHiveColumnHandle> bucketColumnNames;
    // tableBucketNumber is needed, but can be found in tableBucketNumber field of HiveSplit.

    @JsonCreator
    public GlutenBucketConversion(
            @JsonProperty("tableBucketCount") int tableBucketCount,
            @JsonProperty("partitionBucketCount") int partitionBucketCount,
            @JsonProperty("bucketColumnHandles") List<GlutenHiveColumnHandle> bucketColumnHandles)
    {
        this.tableBucketCount = tableBucketCount;
        this.partitionBucketCount = partitionBucketCount;
        this.bucketColumnNames = requireNonNull(bucketColumnHandles, "bucketColumnHandles is null");
    }

    @JsonProperty
    public int getTableBucketCount()
    {
        return tableBucketCount;
    }

    @JsonProperty
    public int getPartitionBucketCount()
    {
        return partitionBucketCount;
    }

    @JsonProperty
    public List<GlutenHiveColumnHandle> getBucketColumnHandles()
    {
        return bucketColumnNames;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GlutenBucketConversion that = (GlutenBucketConversion) o;
        return tableBucketCount == that.tableBucketCount &&
                partitionBucketCount == that.partitionBucketCount &&
                Objects.equals(bucketColumnNames, that.bucketColumnNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableBucketCount, partitionBucketCount, bucketColumnNames);
    }
}
