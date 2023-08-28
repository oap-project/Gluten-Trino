public class HivePartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final int bucketCount;
    private final List<HiveType> hiveTypes;
    private final OptionalInt maxCompatibleBucketCount;
    private final boolean usePartitionedBucketing;

    @JsonCreator
    public HivePartitioningHandle(
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("hiveBucketTypes") List<HiveType> hiveTypes,
            @JsonProperty("maxCompatibleBucketCount") OptionalInt maxCompatibleBucketCount,
            @JsonProperty("usePartitionedBucketing") boolean usePartitionedBucketing)
    {
        this.bucketCount = bucketCount;
        this.hiveTypes = requireNonNull(hiveTypes, "hiveTypes is null");
        this.maxCompatibleBucketCount = maxCompatibleBucketCount;
        this.usePartitionedBucketing = usePartitionedBucketing;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public List<HiveType> getHiveTypes()
    {
        return hiveTypes;
    }

    @JsonProperty
    public OptionalInt getMaxCompatibleBucketCount()
    {
        return maxCompatibleBucketCount;
    }

    @JsonProperty
    public boolean isUsePartitionedBucketing()
    {
        return usePartitionedBucketing;
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .add("buckets", bucketCount)
                .add("hiveTypes", hiveTypes);
        if (usePartitionedBucketing) {
            helper.add("usePartitionedBucketing", usePartitionedBucketing);
        }
        return helper.toString();
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
        HivePartitioningHandle that = (HivePartitioningHandle) o;
        return bucketCount == that.bucketCount &&
                usePartitionedBucketing == that.usePartitionedBucketing &&
                Objects.equals(hiveTypes, that.hiveTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketCount, hiveTypes, usePartitionedBucketing);
    }
}
