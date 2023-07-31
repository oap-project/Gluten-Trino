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
package io.trino.jni;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.UnsafeSliceFactory;
import io.trino.RowPagesBuilder;
import io.trino.execution.buffer.PageSerializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

public final class TrinoBridgeSerializationTest
{
    private static final InternalBlockEncodingSerde BLOCK_ENCODING_SERDE = new InternalBlockEncodingSerde(new BlockEncodingManager(), TESTING_TYPE_MANAGER);

    private TrinoBridgeSerializationTest() {}

    public static void test1(TrinoBridge bridge)
    {
        String id = "test_empty_page";
        long[] address = new long[1];
        int[] length = new int[1];
        bridge.getBuffer(id, address, length);
        Slice sliceNative = UnsafeSliceFactory.getInstance().newSlice(address[0], length[0]);

        List<Type> types = new ArrayList<Type>();
        PageBuilder builder = new PageBuilder(types);
        Page page = builder.build();

        PagesSerdeFactory factory = new PagesSerdeFactory(BLOCK_ENCODING_SERDE, false);
        PageSerializer serializer = factory.createSerializer(Optional.empty());
        Slice sliceJava = serializer.serialize(page);
//        System.out.println( sliceJava.length());
        System.out.println(sliceJava.equals(sliceNative));
    }

    public static void test2(TrinoBridge bridge)
    {
        String id = "test_row_page_1";
        long[] address = new long[1];
        int[] length = new int[1];
        bridge.getBuffer(id, address, length);
        Slice sliceNative = UnsafeSliceFactory.getInstance().newSlice(address[0], length[0]);
        List<Type> types = ImmutableList.of(INTEGER);
        Page page = RowPagesBuilder.rowPagesBuilder(types).row(0).row(1).row(2).build().get(0);

        PagesSerdeFactory factory = new PagesSerdeFactory(BLOCK_ENCODING_SERDE, false);
        PageSerializer serializer = factory.createSerializer(Optional.empty());
        Slice sliceJava = serializer.serialize(page);
//        System.out.println( sliceJava.length());
        System.out.println(sliceJava.equals(sliceNative));
    }

    public static void test3(TrinoBridge bridge)
    {
        String id = "test_row_page_2";
        long[] address = new long[1];
        int[] length = new int[1];
        bridge.getBuffer(id, address, length);
        Slice sliceNative = UnsafeSliceFactory.getInstance().newSlice(address[0], length[0]);
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);
        Page page = RowPagesBuilder.rowPagesBuilder(types).row(0, 0).row(1, null).row(2, 2).row(null, 3).row(4, 4).build().get(0);

        PagesSerdeFactory factory = new PagesSerdeFactory(BLOCK_ENCODING_SERDE, false);
        PageSerializer serializer = factory.createSerializer(Optional.empty());
        Slice sliceJava = serializer.serialize(page);
//        System.out.println( sliceJava.length());
        System.out.println(sliceJava.equals(sliceNative));
    }

    public static void main(String[] args)
    {
        TrinoBridge bridge = new TrinoBridge();
        test1(bridge);
        test2(bridge);
        test3(bridge);
    }
}
