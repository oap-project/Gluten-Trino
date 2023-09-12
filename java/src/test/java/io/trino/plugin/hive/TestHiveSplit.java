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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.plugin.hive.HiveColumnHandle.ColumnType;
import io.trino.plugin.hive.protocol.GlutenHiveSplit;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static io.trino.hive.thrift.metastore.hive_metastoreConstants.BUCKET_COUNT;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.BUCKET_FIELD_NAME;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_OUTPUT_FORMAT;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.META_TABLE_LOCATION;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_DDL;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_LIB;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestHiveSplit
{
    @Test
    public void testJsonRoundTrip()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(new TestingTypeManager())));
        JsonCodec<HiveSplit> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(HiveSplit.class);

        Properties schema = new Properties();
        schema.setProperty("foo", "bar");
        schema.setProperty("bar", "baz");

        ImmutableList<HivePartitionKey> partitionKeys = ImmutableList.of(new HivePartitionKey("a", "apple"), new HivePartitionKey("b", "42"));
        ImmutableList<HostAddress> addresses = ImmutableList.of(HostAddress.fromParts("127.0.0.1", 44), HostAddress.fromParts("127.0.0.1", 45));

        AcidInfo.Builder acidInfoBuilder = AcidInfo.builder(new Path("file:///data/fullacid"));
        acidInfoBuilder.addDeleteDelta(new Path("file:///data/fullacid/delete_delta_0000004_0000004_0000"));
        acidInfoBuilder.addDeleteDelta(new Path("file:///data/fullacid/delete_delta_0000007_0000007_0000"));
        AcidInfo acidInfo = acidInfoBuilder.build().get();

        HiveSplit expected = new HiveSplit(
                "db",
                "table",
                "partitionId",
                "path",
                42,
                87,
                88,
                Instant.now().toEpochMilli(),
                schema,
                partitionKeys,
                addresses,
                OptionalInt.empty(),
                OptionalInt.empty(),
                0,
                true,
                TableToPartitionMapping.mapColumnsByIndex(ImmutableMap.of(1, new HiveTypeName("string"))),
                Optional.of(new HiveSplit.BucketConversion(
                        BUCKETING_V1,
                        32,
                        16,
                        ImmutableList.of(createBaseColumn("col", 5, HIVE_LONG, BIGINT, ColumnType.REGULAR, Optional.of("comment"))))),
                Optional.empty(),
                false,
                Optional.of(acidInfo),
                555534,
                SplitWeight.fromProportion(2.0)); // some non-standard value

        String json = codec.toJson(expected);
        HiveSplit actual = codec.fromJson(json);

        assertEquals(actual.getDatabase(), expected.getDatabase());
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getPartitionName(), expected.getPartitionName());
        assertEquals(actual.getPath(), expected.getPath());
        assertEquals(actual.getStart(), expected.getStart());
        assertEquals(actual.getLength(), expected.getLength());
        assertEquals(actual.getEstimatedFileSize(), expected.getEstimatedFileSize());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getPartitionKeys(), expected.getPartitionKeys());
        assertEquals(actual.getAddresses(), expected.getAddresses());
        assertEquals(actual.getTableToPartitionMapping().getPartitionColumnCoercions(), expected.getTableToPartitionMapping().getPartitionColumnCoercions());
        assertEquals(actual.getTableToPartitionMapping().getTableToPartitionColumns(), expected.getTableToPartitionMapping().getTableToPartitionColumns());
        assertEquals(actual.getBucketConversion(), expected.getBucketConversion());
        assertEquals(actual.isForceLocalScheduling(), expected.isForceLocalScheduling());
        assertEquals(actual.isS3SelectPushdownEnabled(), expected.isS3SelectPushdownEnabled());
        assertEquals(actual.getAcidInfo().get(), expected.getAcidInfo().get());
        assertEquals(actual.getSplitNumber(), expected.getSplitNumber());
        assertEquals(actual.getSplitWeight(), expected.getSplitWeight());
    }

    @Test
    public void testGlutenHiveSplit()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(new TestingTypeManager())));
        JsonCodec<GlutenHiveSplit> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(GlutenHiveSplit.class);

        Properties schema = new Properties();
        schema.setProperty("foo", "bar");
        schema.setProperty("bar", "baz");
        schema.setProperty(FILE_INPUT_FORMAT, "");
        schema.setProperty(FILE_OUTPUT_FORMAT, "");
        schema.setProperty(SERIALIZATION_LIB, "");
        schema.setProperty(META_TABLE_LOCATION, "");
        schema.setProperty(BUCKET_FIELD_NAME, "");
        schema.setProperty(BUCKET_COUNT, "0");
        schema.setProperty(SERIALIZATION_DDL, "struct table { i64 id, string name}");

        ImmutableList<HivePartitionKey> partitionKeys = ImmutableList.of(new HivePartitionKey("a", "apple"), new HivePartitionKey("b", "42"));
        ImmutableList<HostAddress> addresses = ImmutableList.of(HostAddress.fromParts("127.0.0.1", 44), HostAddress.fromParts("127.0.0.1", 45));

        AcidInfo.Builder acidInfoBuilder = AcidInfo.builder(new Path("file:///data/fullacid"));
        acidInfoBuilder.addDeleteDelta(new Path("file:///data/fullacid/delete_delta_0000004_0000004_0000"));
        acidInfoBuilder.addDeleteDelta(new Path("file:///data/fullacid/delete_delta_0000007_0000007_0000"));
        AcidInfo acidInfo = acidInfoBuilder.build().get();

        HiveSplit sourceHiveSplit = new HiveSplit(
                "db",
                "table",
                "partitionId",
                "path",
                42,
                87,
                88,
                Instant.now().toEpochMilli(),
                schema,
                partitionKeys,
                addresses,
                OptionalInt.empty(),
                OptionalInt.empty(),
                0,
                true,
                TableToPartitionMapping.mapColumnsByIndex(ImmutableMap.of(1, new HiveTypeName("string"))),
                Optional.of(new HiveSplit.BucketConversion(
                        BUCKETING_V1,
                        32,
                        16,
                        ImmutableList.of(createBaseColumn("col", 5, HIVE_LONG, BIGINT, ColumnType.REGULAR, Optional.of("comment"))))),
                Optional.empty(),
                false,
                Optional.of(acidInfo),
                555534,
                SplitWeight.fromProportion(2.0)); // some non-standard value
        GlutenHiveSplit glutenHiveSplit = sourceHiveSplit.getProtocol();
        String json = codec.toJson(glutenHiveSplit);

        System.out.println(json);
    }
}
