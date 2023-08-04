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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.protocol.GlutenColumnHandle;
import io.trino.spi.protocol.GlutenConnectorSplit;
import io.trino.spi.protocol.GlutenNodeSelectionStrategy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class GlutenHiveSplit
        implements GlutenConnectorSplit
{
    private final GlutenHiveFileSplit fileSplit;
    private final GlutenStorage storage;
    private final List<HivePartitionKey> partitionKeys;
    private final List<HostAddress> addresses;
    private final String database;
    private final String table;
    private final String partitionName;
    private final OptionalInt readBucketNumber;
    private final OptionalInt tableBucketNumber;
    private final GlutenNodeSelectionStrategy nodeSelectionStrategy;
    private final int partitionDataColumnCount;
    private final GlutenTableToPartitionMapping tableToPartitionMapping;
    private final Optional<GlutenBucketConversion> bucketConversion;
    private final boolean s3SelectPushdownEnabled;
    private final GlutenCacheQuotaRequirement cacheQuotaRequirement;
    private final Optional<GlutenEncryptionInformation> encryptionInformation;
    private final Set<GlutenColumnHandle> redundantColumnDomains;
    private final SplitWeight splitWeight;

    @JsonCreator
    public GlutenHiveSplit(
            @JsonProperty("fileSplit") GlutenHiveFileSplit fileSplit,
            @JsonProperty("database") String database,
            @JsonProperty("table") String table,
            @JsonProperty("partitionName") String partitionName,
            @JsonProperty("storage") GlutenStorage storage,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("readBucketNumber") OptionalInt readBucketNumber,
            @JsonProperty("tableBucketNumber") OptionalInt tableBucketNumber,
            @JsonProperty("nodeSelectionStrategy") GlutenNodeSelectionStrategy nodeSelectionStrategy,
            @JsonProperty("partitionDataColumnCount") int partitionDataColumnCount,
            @JsonProperty("tableToPartitionMapping") GlutenTableToPartitionMapping tableToPartitionMapping,
            @JsonProperty("bucketConversion") Optional<GlutenBucketConversion> bucketConversion,
            @JsonProperty("s3SelectPushdownEnabled") boolean s3SelectPushdownEnabled,
            @JsonProperty("cacheQuota") GlutenCacheQuotaRequirement cacheQuotaRequirement,
            @JsonProperty("encryptionMetadata") Optional<GlutenEncryptionInformation> encryptionInformation,
            @JsonProperty("redundantColumnDomains") Set<GlutenColumnHandle> redundantColumnDomains,
            @JsonProperty("splitWeight") SplitWeight splitWeight)
    {
        requireNonNull(fileSplit, "fileSplit is null");
        requireNonNull(database, "database is null");
        requireNonNull(table, "table is null");
        requireNonNull(partitionName, "partitionName is null");
        requireNonNull(storage, "storage is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        requireNonNull(addresses, "addresses is null");
        requireNonNull(readBucketNumber, "readBucketNumber is null");
        requireNonNull(tableBucketNumber, "tableBucketNumber is null");
        requireNonNull(nodeSelectionStrategy, "nodeSelectionStrategy is null");
        requireNonNull(tableToPartitionMapping, "tableToPartitionMapping is null");
        requireNonNull(bucketConversion, "bucketConversion is null");
        requireNonNull(cacheQuotaRequirement, "cacheQuotaRequirement is null");
        requireNonNull(encryptionInformation, "encryptionMetadata is null");
        requireNonNull(redundantColumnDomains, "redundantColumnDomains is null");

        this.fileSplit = fileSplit;
        this.database = database;
        this.table = table;
        this.partitionName = partitionName;
        this.storage = storage;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.addresses = ImmutableList.copyOf(addresses);
        this.readBucketNumber = readBucketNumber;
        this.tableBucketNumber = tableBucketNumber;
        this.nodeSelectionStrategy = nodeSelectionStrategy;
        this.partitionDataColumnCount = partitionDataColumnCount;
        this.tableToPartitionMapping = tableToPartitionMapping;
        this.bucketConversion = bucketConversion;
        this.s3SelectPushdownEnabled = s3SelectPushdownEnabled;
        this.cacheQuotaRequirement = cacheQuotaRequirement;
        this.encryptionInformation = encryptionInformation;
        this.redundantColumnDomains = ImmutableSet.copyOf(redundantColumnDomains);
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
    }

    @JsonProperty
    public GlutenHiveFileSplit getFileSplit()
    {
        return fileSplit;
    }

    @JsonProperty
    public String getDatabase()
    {
        return database;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getPartitionName()
    {
        return partitionName;
    }

    @JsonProperty
    public GlutenStorage getStorage()
    {
        return storage;
    }

    @JsonProperty
    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public OptionalInt getReadBucketNumber()
    {
        return readBucketNumber;
    }

    @JsonProperty
    public OptionalInt getTableBucketNumber()
    {
        return tableBucketNumber;
    }

    @JsonProperty
    public int getPartitionDataColumnCount()
    {
        return partitionDataColumnCount;
    }

    @JsonProperty
    public GlutenTableToPartitionMapping getTableToPartitionMapping()
    {
        return tableToPartitionMapping;
    }

    @JsonProperty
    public Optional<GlutenBucketConversion> getBucketConversion()
    {
        return bucketConversion;
    }

    @JsonProperty
    public GlutenNodeSelectionStrategy getNodeSelectionStrategy()
    {
        return nodeSelectionStrategy;
    }

    @JsonProperty
    public boolean isS3SelectPushdownEnabled()
    {
        return s3SelectPushdownEnabled;
    }

    @JsonProperty
    public GlutenCacheQuotaRequirement getCacheQuotaRequirement()
    {
        return cacheQuotaRequirement;
    }

    @JsonProperty
    public Optional<GlutenEncryptionInformation> getEncryptionInformation()
    {
        return encryptionInformation;
    }

    @JsonProperty
    public Set<GlutenColumnHandle> getRedundantColumnDomains()
    {
        return redundantColumnDomains;
    }

    @JsonProperty
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }
}
