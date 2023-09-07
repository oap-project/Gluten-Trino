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

#include "functions/TpchPartitionFunction.h"
#include "gtest/gtest.h"
#include "velox/buffer/Buffer.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

class TpchPartitionFunctionTest : public ::testing::Test {
 protected:
  void assertPartitions(const VectorPtr& vector,
                        const std::vector<int32_t>& bucketToPartition,
                        const std::vector<uint32_t>& expectedPartitions) {
    auto rowVector = makeRowVector({vector});

    auto size = rowVector->size();
    auto rowsPerCount = totalRows / bucketToPartition.size();

    io::trino::bridge::TpchPartitionFunction partitionFunction(rowsPerCount,
                                                               bucketToPartition);
    std::vector<uint32_t> partitions(size);
    partitionFunction.partition(*rowVector, partitions);
    for (auto i = 0; i < size; ++i) {
      EXPECT_EQ(expectedPartitions[i], partitions[i])
          << "at " << i << ": " << vector->toString(i);
    }
  }

  RowVectorPtr makeRowVector(const std::vector<VectorPtr>& children) {
    std::vector<std::string> names;
    for (int32_t i = 0; i < children.size(); ++i) {
      names.push_back(fmt::format("c{}", i));
    }

    std::vector<std::shared_ptr<const Type>> childTypes;
    childTypes.resize(children.size());
    for (int i = 0; i < children.size(); i++) {
      childTypes[i] = children[i]->type();
    }
    auto rowType = ROW(std::move(names), std::move(childTypes));
    const size_t vectorSize = children.empty() ? 0 : children.front()->size();

    return std::make_shared<RowVector>(pool_.get(), rowType, BufferPtr(nullptr),
                                       vectorSize, children);
  }

  template <typename T, typename Type = typename CppToType<T>::NativeType>
  FlatVectorPtr<Type> makeNullableFlatVector(
      const std::vector<std::optional<T>>& data,
      const TypePtr& type = CppToType<T>::create()) {
    BufferPtr dataBuffer = AlignedBuffer::allocate<Type>(data.size(), pool_.get());

    auto flatVector = std::make_shared<FlatVector<Type>>(
        pool_.get(), type, BufferPtr(nullptr), data.size(), std::move(dataBuffer),
        std::vector<BufferPtr>());

    for (vector_size_t i = 0; i < data.size(); i++) {
      if (data[i] != std::nullopt) {
        flatVector->set(i, Type(*data[i]));
        flatVector->setNull(i, false);
      } else {
        flatVector->set(i, Type());
        flatVector->setNull(i, true);
      }
    }
    return flatVector;
  }

  // translate from 'io.trino.tpch.OrderGenerator#makeOrderKey'
  int64_t makeOrderKey(int64_t index) {
    int64_t lowBits = index & ((1 << ORDER_KEY_SPARSE_KEEP) - 1);

    int64_t ok = index;
    ok >>= ORDER_KEY_SPARSE_KEEP;
    ok <<= ORDER_KEY_SPARSE_BITS;
    ok <<= ORDER_KEY_SPARSE_KEEP;
    ok += lowBits;

    return ok;
  }

  const int32_t totalRows = 12345;
  const int32_t ORDER_KEY_SPARSE_KEEP = 3;
  const int32_t ORDER_KEY_SPARSE_BITS = 2;

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::defaultMemoryManager().addLeafPool("TpchPartitionFunctionTest");
};

TEST_F(TpchPartitionFunctionTest, partition) {
  auto values = makeNullableFlatVector<int64_t>(
      {std::nullopt, makeOrderKey(11), makeOrderKey(2346), makeOrderKey(4391),
       makeOrderKey(9877), makeOrderKey(5688), makeOrderKey(12345), makeOrderKey(7813)});
  assertPartitions(values, {0}, {0, 0, 0, 0, 0, 0, 0, 0});
  assertPartitions(values, {0, 1}, {0, 0, 0, 0, 1, 0, 1, 1});
  assertPartitions(values, {0, 1, 2}, {0, 0, 0, 1, 2, 1, 2, 1});
  assertPartitions(values, {0, 1, 2, 3}, {0, 0, 0, 1, 3, 1, 3, 2});
  assertPartitions(values, {0, 1, 2, 3, 4}, {0, 0, 0, 1, 4, 2, 4, 3});
  assertPartitions(values, {0, 1, 2, 3, 4, 5}, {0, 0, 1, 2, 4, 2, 5, 3});
}

TEST_F(TpchPartitionFunctionTest, spec) {
  Type::registerSerDe();
  core::ITypedExpr::registerSerDe();

  std::vector<int32_t> bucketToPartition{0, 1, 2, 3, 4, 5};
  int64_t rowsPerBucket = totalRows / bucketToPartition.size();

  auto tpchSpec = std::make_unique<io::trino::bridge::TpchPartitionFunctionSpec>(
      rowsPerBucket, bucketToPartition);
  ASSERT_EQ(tpchSpec->toString(), "TPCH");

  auto serialized = tpchSpec->serialize();
  ASSERT_EQ(serialized["name"], "TpchPartitionFunctionSpec");
  ASSERT_EQ(serialized["rowsPerBucket"], rowsPerBucket);

  auto copy =
      io::trino::bridge::TpchPartitionFunctionSpec::deserialize(serialized, pool_.get());
  ASSERT_EQ(tpchSpec->toString(), copy->toString());
}
