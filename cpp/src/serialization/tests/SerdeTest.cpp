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

#include <gtest/gtest.h>
#include "serialization/TrinoBuffer.h"
#include "serialization/TrinoBufferStream.h"
#include "serialization/TrinoSerializer.h"

#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace io::trino::bridge;

class TrinoSerializerTest : public testing::Test {
 protected:
  static void SetUpTestCase() { TrinoVectorSerde::registerVectorSerde(); }

  void SetUp() override {
    pool_ = memory::addDefaultLeafMemoryPool();
    serde_ = std::make_unique<TrinoVectorSerde>();
    vectorMaker_ = std::make_unique<facebook::velox::test::VectorMaker>(pool_.get());
  }

  void sanityCheckEstimateSerializedSize(const RowVectorPtr& rowVector) {
    const auto numRows = rowVector->size();

    std::vector<IndexRange> rows(numRows);
    for (int i = 0; i < numRows; i++) {
      rows[i] = IndexRange{i, 1};
    }

    std::vector<vector_size_t> rowSizes(numRows, 0);
    std::vector<vector_size_t*> rawRowSizes(numRows);
    for (auto i = 0; i < numRows; i++) {
      rawRowSizes[i] = &rowSizes[i];
    }
    serde_->estimateSerializedSize(rowVector, folly::Range(rows.data(), numRows),
                                   rawRowSizes.data());
  }

  void serialize(const RowVectorPtr& rowVector, std::ostream* output) {
    auto streamInitialSize = output->tellp();
    sanityCheckEstimateSerializedSize(rowVector);

    auto arena = std::make_unique<StreamArena>(pool_.get());
    auto rowType = asRowType(rowVector->type());
    auto numRows = rowVector->size();
    auto serializer = serde_->createSerializer(rowType, numRows, arena.get(), nullptr);

    serializer->append(rowVector);
    auto size = serializer->maxSerializedSize();
    TrinoOutputStreamListener listener;
    OStreamOutputStream out(output, &listener);
    serializer->flush(&out);
    ASSERT_EQ(size, out.tellp() - streamInitialSize);
  }

  std::unique_ptr<ByteStream> toByteStream(const std::string& input) {
    auto byteStream = std::make_unique<ByteStream>();
    ByteRange byteRange{reinterpret_cast<uint8_t*>(const_cast<char*>(input.data())),
                        (int32_t)input.length(), 0};
    byteStream->resetInput({byteRange});
    return byteStream;
  }

  RowVectorPtr deserialize(const RowTypePtr& rowType, const std::string& input) {
    auto byteStream = toByteStream(input);
    RowVectorPtr result;
    serde_->deserialize(byteStream.get(), pool_.get(), rowType, &result, nullptr);
    return result;
  }

  RowVectorPtr makeTestVector(vector_size_t size) {
    auto a =
        vectorMaker_->flatVector<int64_t>(size, [](vector_size_t row) { return row; });
    auto b = vectorMaker_->flatVector<double>(
        size, [](vector_size_t row) { return row * 0.1; });

    std::vector<VectorPtr> childVectors = {a, b};

    return vectorMaker_->rowVector(childVectors);
  }

  void testRoundTrip(VectorPtr vector) {
    auto rowVector = vectorMaker_->rowVector({vector});
    std::ostringstream out;
    serialize(rowVector, &out);

    auto rowType = asRowType(rowVector->type());
    auto deserialized = deserialize(rowType, out.str());
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<TrinoVectorSerde> serde_;
  std::unique_ptr<facebook::velox::test::VectorMaker> vectorMaker_;
};

TEST_F(TrinoSerializerTest, basic) {
  vector_size_t numRows = 1000;
  auto rowVector = makeTestVector(numRows);
  testRoundTrip(rowVector);
}

TEST_F(TrinoSerializerTest, dictionaryWithExtraNulls) {
  vector_size_t size = 1'000;

  auto base = vectorMaker_->flatVector<int64_t>(10, [](auto row) { return row; });

  BufferPtr nulls = AlignedBuffer::allocate<bool>(size, pool_.get());
  auto rawNulls = nulls->asMutable<uint64_t>();
  for (auto i = 0; i < size; i++) {
    bits::setNull(rawNulls, i, i % 5 == 0);
  }

  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; i++) {
    if (i % 5 != 0) {
      rawIndices[i] = i % 10;
    }
  }

  auto dictionary = BaseVector::wrapInDictionary(nulls, indices, size, base);
  testRoundTrip(dictionary);
}

TEST_F(TrinoSerializerTest, emptyPage) {
  auto rowVector = vectorMaker_->rowVector(ROW({"a"}, {BIGINT()}), 0);

  std::ostringstream out;
  serialize(rowVector, &out);

  auto rowType = asRowType(rowVector->type());
  auto deserialized = deserialize(rowType, out.str());
  facebook::velox::test::assertEqualVectors(deserialized, rowVector);
}

TEST_F(TrinoSerializerTest, emptyArray) {
  auto arrayVector = vectorMaker_->arrayVector<int32_t>(
      1'000, [](vector_size_t row) { return row % 5; },
      [](vector_size_t row) { return row; });

  testRoundTrip(arrayVector);
}

TEST_F(TrinoSerializerTest, multiPage) {
  std::ostringstream out;

  // page 1
  auto a = makeTestVector(1'234);
  serialize(a, &out);

  // page 2
  auto b = makeTestVector(538);
  serialize(b, &out);

  // page 3
  auto c = makeTestVector(2'048);
  serialize(c, &out);

  auto bytes = out.str();

  auto rowType = asRowType(a->type());
  auto byteStream = toByteStream(bytes);

  RowVectorPtr deserialized;
  serde_->deserialize(byteStream.get(), pool_.get(), rowType, &deserialized);
  ASSERT_FALSE(byteStream->atEnd());
  facebook::velox::test::assertEqualVectors(deserialized, a);

  serde_->deserialize(byteStream.get(), pool_.get(), rowType, &deserialized);
  facebook::velox::test::assertEqualVectors(deserialized, b);
  ASSERT_FALSE(byteStream->atEnd());

  serde_->deserialize(byteStream.get(), pool_.get(), rowType, &deserialized);
  facebook::velox::test::assertEqualVectors(deserialized, c);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  err = RUN_ALL_TESTS();

  return err;
  err = RUN_ALL_TESTS();

  return err;
}
