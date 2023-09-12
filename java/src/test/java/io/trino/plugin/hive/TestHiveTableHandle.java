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
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.protocol.GlutenHiveTableHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.testng.Assert.assertEquals;

public class TestHiveTableHandle
{
    private static final Column BUCKET_COLUMN = new Column("id", HIVE_LONG, Optional.empty());
    private static final Column PARTITION_COLUMN = new Column("date", HIVE_INT, Optional.empty());

    private static final HiveColumnHandle BUCKET_HIVE_COLUMN_HANDLE = new HiveColumnHandle(
            BUCKET_COLUMN.getName(),
            0,
            BUCKET_COLUMN.getType(),
            BUCKET_COLUMN.getType().getType(TESTING_TYPE_MANAGER),
            Optional.empty(),
            REGULAR,
            Optional.empty());
    private static final HiveColumnHandle PARTITION_HIVE_COLUMN_HANDLE = new HiveColumnHandle(
            PARTITION_COLUMN.getName(),
            0,
            PARTITION_COLUMN.getType(),
            PARTITION_COLUMN.getType().getType(TESTING_TYPE_MANAGER),
            Optional.empty(),
            PARTITION_KEY,
            Optional.empty());

    @Test
    public void testRoundTrip()
    {
        JsonCodec<HiveTableHandle> codec = JsonCodec.jsonCodec(HiveTableHandle.class);

        HiveTableHandle expected = new HiveTableHandle("schema", "table", ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());

        String json = codec.toJson(expected);
        HiveTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
    }

    @Test
    public void testGlutenHiveTableHandle()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(new TestingTypeManager())));
        JsonCodec<GlutenHiveTableHandle> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(GlutenHiveTableHandle.class);

        Map<HiveColumnHandle, Domain> domains = new HashMap<>();
        domains.put(BUCKET_HIVE_COLUMN_HANDLE, Domain.onlyNull(BIGINT));

        HiveTableHandle hiveTableHandle = new HiveTableHandle(
                "db",
                "table",
                ImmutableList.of(),
                ImmutableList.of(BUCKET_HIVE_COLUMN_HANDLE),
                TupleDomain.withColumnDomains(domains),
                TupleDomain.all(),
                Optional.of(new HiveBucketHandle(
                        ImmutableList.of(BUCKET_HIVE_COLUMN_HANDLE),
                        BUCKETING_V1,
                        20,
                        20,
                        ImmutableList.of())),
                Optional.empty(),
                Optional.empty(),
                NO_ACID_TRANSACTION
        );
        GlutenHiveTableHandle glutenHiveTableHandle = hiveTableHandle.getProtocol();
        String json = codec.toJson(glutenHiveTableHandle);

        System.out.println(json);
    }
}
