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

#include "gtest/gtest.h"
#include "velox/buffer/Buffer.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/RowContainer.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace io::trino::bridge {
extern void registerTrinoSumAggregate(const std::string& prefix);
}

class GroupsHolder {
 public:
  GroupsHolder(int32_t _numGroups, int32_t _size) : numGroups(_numGroups), size(_size) {
    groups = new char*[numGroups];
    for (int i = 0; i < numGroups; ++i) {
      groups[i] = new char[size];
      memset(groups[i], 0, size);
    }
  }
  ~GroupsHolder() {
    for (int i = 0; i < numGroups; ++i) {
      delete[] groups[i];
    }
    delete[] groups;
  }
  char* getGroup(int32_t index) { return groups[index]; }
  char** getGroups() { return groups; }

 private:
  int32_t numGroups;
  int32_t size;
  char** groups;
};

class TrinoSumAggregateTest : public ::testing::Test {
 protected:
  TrinoSumAggregateTest() : queryConfig({}) {
    io::trino::bridge::registerTrinoSumAggregate("test.sum");
    auto aggregateEntry = exec::getAggregateFunctionEntry("test.sum");
    factory = aggregateEntry->factory;
    signatures = aggregateEntry->signatures;
  }

  std::pair<std::unique_ptr<exec::Aggregate>, int32_t> getAggregate(
      core::AggregationNode::Step step, const std::vector<TypePtr>& types,
      const TypePtr& resultType) {
    auto aggregate = factory(step, types, resultType, queryConfig);

    int32_t rowSizeOffset = bits::nbytes(1);
    int32_t offset = rowSizeOffset + sizeof(uint32_t);
    offset = bits::roundUp(offset, aggregate->accumulatorAlignmentSize());
    aggregate->setOffsets(offset, exec::RowContainer::nullByte(0),
                          exec::RowContainer::nullMask(0), rowSizeOffset);

    return std::make_pair(std::move(aggregate), offset);
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

    return std::make_shared<RowVector>(pool.get(), rowType, BufferPtr(nullptr),
                                       vectorSize, children);
  }

  template <typename T, typename Type = typename CppToType<T>::NativeType>
  FlatVectorPtr<Type> makeNullableFlatVector(
      const std::vector<std::optional<T>>& data,
      const TypePtr& type = CppToType<T>::create()) {
    BufferPtr dataBuffer = AlignedBuffer::allocate<Type>(data.size(), pool.get());

    auto flatVector = std::make_shared<FlatVector<Type>>(
        pool.get(), type, BufferPtr(nullptr), data.size(), std::move(dataBuffer),
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

  template <typename T, typename Type = typename CppToType<T>::NativeType>
  VectorPtr makeConstant(T value, vector_size_t size,
                         const TypePtr& type = CppToType<T>::create()) {
    return std::make_shared<ConstantVector<Type>>(pool.get(), size, false, type,
                                                  std::move(value));
  }

  VectorPtr makeNullConstant(TypeKind typeKind, vector_size_t size) {
    return BaseVector::createNullConstant(createType(typeKind, {}), size, pool.get());
  }

  template <typename TInput, typename TAccumulator, typename TResult>
  void checkPartialSum(int32_t numGroups, bool constant,
                       const std::vector<TypePtr>& inputTypes, const TypePtr& resultType,
                       const std::vector<std::optional<TInput>>& data) {
    auto [aggregate, offset] =
        getAggregate(core::AggregationNode::Step::kPartial, inputTypes, resultType);
    ASSERT_TRUE(!!aggregate);

    auto size = offset + aggregate->accumulatorFixedWidthSize();
    GroupsHolder groups(numGroups, size);

    std::vector<char*> rowToGroups(data.size());
    std::vector<TResult> groupSums(numGroups, 0);
    std::vector<int64_t> groupCounts(numGroups, 0);
    for (auto i = 0; i < data.size(); ++i) {
      rowToGroups[i] = groups.getGroup(i % numGroups);
      groupSums[i % numGroups] += data[i].has_value() ? data[i].value() : 0;
      groupCounts[i % numGroups] += data[i].has_value() ? 1 : 0;
    }

    std::vector<vector_size_t> indices(numGroups);
    for (vector_size_t i = 0; i < numGroups; ++i) {
      indices.push_back(i);
    }
    aggregate->initializeNewGroups(groups.getGroups(), indices);

    auto sumVector = makeNullableFlatVector<TAccumulator>(
        std::vector<std::optional<TAccumulator>>(numGroups, 0));
    auto countVector = makeNullableFlatVector<int64_t>(
        std::vector<std::optional<int64_t>>(numGroups, 0));
    std::vector<VectorPtr> children{sumVector, countVector};
    auto accumulators = makeRowVector(children);
    aggregate->extractAccumulators(groups.getGroups(), numGroups,
                                   reinterpret_cast<VectorPtr*>(&accumulators));
    ASSERT_EQ(accumulators->size(), numGroups);
    ASSERT_EQ(sumVector->size(), numGroups);
    ASSERT_EQ(countVector->size(), numGroups);
    for (int32_t i = 0; i < numGroups; ++i) {
      ASSERT_EQ(sumVector->valueAt(i), 0);
      ASSERT_EQ(countVector->valueAt(i), 0);
    }

    if (!constant) {
      VectorPtr input = makeNullableFlatVector<TInput>(data);
      SelectivityVector rows{input->size()};
      aggregate->addRawInput(rowToGroups.data(), rows, {input}, false);
    } else {
      for (auto i = 0; i < data.size(); ++i) {
        auto value = data[i].has_value()
                         ? makeConstant<TInput>(data[i].value(), 1)
                         : makeNullConstant(CppToType<TInput>::typeKind, 1);
        SelectivityVector rows{value->size()};
        aggregate->addSingleGroupRawInput(groups.getGroup(i % numGroups), rows, {value},
                                          false);
      }
    }

    aggregate->extractAccumulators(groups.getGroups(), numGroups,
                                   reinterpret_cast<VectorPtr*>(&accumulators));
    ASSERT_EQ(accumulators->size(), numGroups);
    ASSERT_EQ(sumVector->size(), numGroups);
    ASSERT_EQ(countVector->size(), numGroups);
    for (int32_t i = 0; i < numGroups; ++i) {
      ASSERT_EQ(sumVector->valueAt(i), groupSums[i]);
      ASSERT_EQ(countVector->valueAt(i), groupCounts[i]);
    }

    auto result = makeNullableFlatVector<TResult>({std::nullopt});
    aggregate->extractValues(groups.getGroups(), numGroups,
                             reinterpret_cast<VectorPtr*>(&result));
    ASSERT_EQ(result->size(), numGroups);
    for (int32_t i = 0; i < numGroups; ++i) {
      ASSERT_EQ(result->valueAt(i), groupSums[i]);
    }
  }

  template <typename TInput, typename TAccumulator, typename TResult>
  void checkFinalSum(int32_t numGroups, bool constant,
                     const std::vector<TypePtr>& inputTypes, const TypePtr& resultType,
                     const std::vector<std::optional<TAccumulator>>& sumData,
                     const std::vector<std::optional<int64_t>>& sumCount) {
    ASSERT_EQ(sumData.size(), sumCount.size());

    auto [aggregate, offset] =
        getAggregate(core::AggregationNode::Step::kFinal, inputTypes, resultType);
    ASSERT_TRUE(!!aggregate);

    auto size = offset + aggregate->accumulatorFixedWidthSize();
    GroupsHolder groups(numGroups, size);

    std::vector<char*> rowToGroups(sumData.size());
    std::vector<TResult> groupSums(numGroups, 0);
    std::vector<int64_t> groupCounts(numGroups, 0);
    for (auto i = 0; i < sumData.size(); ++i) {
      rowToGroups[i] = groups.getGroup(i % numGroups);
      groupSums[i % numGroups] += sumData[i].has_value() ? sumData[i].value() : 0;
      groupCounts[i % numGroups] += sumCount[i].has_value() ? sumCount[i].value() : 0;
    }

    std::vector<vector_size_t> indices(numGroups);
    for (vector_size_t i = 0; i < numGroups; ++i) {
      indices.push_back(i);
    }
    aggregate->initializeNewGroups(groups.getGroups(), indices);

    auto sumVector = makeNullableFlatVector<TAccumulator>(
        std::vector<std::optional<TAccumulator>>(numGroups, 0));
    auto countVector = makeNullableFlatVector<int64_t>(
        std::vector<std::optional<int64_t>>(numGroups, 0));
    std::vector<VectorPtr> accumulatorChildren{sumVector, countVector};
    auto accumulators = makeRowVector(accumulatorChildren);

    aggregate->extractAccumulators(groups.getGroups(), numGroups,
                                   reinterpret_cast<VectorPtr*>(&accumulators));
    ASSERT_EQ(accumulators->size(), numGroups);
    ASSERT_EQ(sumVector->size(), numGroups);
    ASSERT_EQ(countVector->size(), numGroups);
    for (int32_t i = 0; i < numGroups; ++i) {
      ASSERT_EQ(sumVector->valueAt(i), 0);
      ASSERT_EQ(countVector->valueAt(i), 0);
    }

    if (!constant) {
      VectorPtr sum = makeNullableFlatVector<TAccumulator>(sumData);
      VectorPtr count = makeNullableFlatVector<int64_t>(sumCount);
      auto intermediateResults = makeRowVector({sum, count});
      SelectivityVector rows{intermediateResults->size()};
      aggregate->addIntermediateResults(rowToGroups.data(), rows, {intermediateResults},
                                        false);
    } else {
      for (auto i = 0; i < sumData.size(); ++i) {
        auto sum = sumData[i].has_value()
                       ? makeConstant<TAccumulator>(sumData[i].value(), 1)
                       : makeNullConstant(CppToType<TAccumulator>::typeKind, 1);
        auto count = sumCount[i].has_value()
                         ? makeConstant<int64_t>(sumCount[i].value(), 1)
                         : makeNullConstant(TypeKind::BIGINT, 1);
        auto intermediateResult = makeRowVector({sum, count});
        SelectivityVector rows{intermediateResult->size()};
        aggregate->addSingleGroupIntermediateResults(groups.getGroup(i % numGroups), rows,
                                                     {intermediateResult}, false);
      }
    }

    aggregate->extractAccumulators(groups.getGroups(), numGroups,
                                   reinterpret_cast<VectorPtr*>(&accumulators));
    ASSERT_EQ(accumulators->size(), numGroups);
    ASSERT_EQ(sumVector->size(), numGroups);
    ASSERT_EQ(countVector->size(), numGroups);
    for (int32_t i = 0; i < numGroups; ++i) {
      ASSERT_EQ(sumVector->valueAt(i), groupSums[i]);
      ASSERT_EQ(countVector->valueAt(i), groupCounts[i]);
    }

    auto result = makeNullableFlatVector<TResult>({std::nullopt});
    aggregate->extractValues(groups.getGroups(), numGroups,
                             reinterpret_cast<VectorPtr*>(&result));
    ASSERT_EQ(result->size(), numGroups);
    for (int32_t i = 0; i < numGroups; ++i) {
      ASSERT_EQ(result->valueAt(i), groupSums[i]);
    }
  }

  core::QueryConfig queryConfig;
  exec::AggregateFunctionFactory factory;
  std::vector<exec::AggregateFunctionSignaturePtr> signatures;

  std::shared_ptr<memory::MemoryPool> pool =
      memory::defaultMemoryManager().addLeafPool("TrinoSumAggregateTest");
};

TEST_F(TrinoSumAggregateTest, partial) {
  for (int numGroups = 1; numGroups <= 5; ++numGroups) {
    checkPartialSum<int16_t, int64_t, int64_t>(numGroups, false, {SMALLINT()}, BIGINT(),
                                               {1, 2, 3, 4, 5});
    checkPartialSum<int32_t, int64_t, int64_t>(numGroups, false, {INTEGER()}, BIGINT(),
                                               {1, 2, 3, 4, 5});
    checkPartialSum<int64_t, int64_t, int64_t>(numGroups, false, {BIGINT()}, BIGINT(),
                                               {1, 2, 3, 4, 5});
    checkPartialSum<float, double, float>(numGroups, false, {REAL()}, DOUBLE(),
                                          {1, 2, 3, 4, 5});
    checkPartialSum<double, double, double>(numGroups, false, {DOUBLE()}, DOUBLE(),
                                            {1, 2, 3, 4, 5});
  }
}

TEST_F(TrinoSumAggregateTest, partialWithNull) {
  for (int numGroups = 1; numGroups <= 5; ++numGroups) {
    checkPartialSum<int16_t, int64_t, int64_t>(numGroups, false, {SMALLINT()}, BIGINT(),
                                               {1, 2, 3, 4, 5, std::nullopt});
    checkPartialSum<int32_t, int64_t, int64_t>(numGroups, false, {INTEGER()}, BIGINT(),
                                               {1, 2, 3, 4, 5, std::nullopt});
    checkPartialSum<int64_t, int64_t, int64_t>(numGroups, false, {BIGINT()}, BIGINT(),
                                               {1, 2, 3, 4, 5, std::nullopt});
    checkPartialSum<float, double, float>(numGroups, false, {REAL()}, DOUBLE(),
                                          {1, 2, 3, 4, 5, std::nullopt});
    checkPartialSum<double, double, double>(numGroups, false, {DOUBLE()}, DOUBLE(),
                                            {1, 2, 3, 4, 5, std::nullopt});
  }
}

TEST_F(TrinoSumAggregateTest, partialWithConstant) {
  for (int numGroups = 1; numGroups <= 5; ++numGroups) {
    checkPartialSum<int16_t, int64_t, int64_t>(numGroups, true, {SMALLINT()}, BIGINT(),
                                               {1, 2, 3, 4, 5, std::nullopt});
    checkPartialSum<int32_t, int64_t, int64_t>(numGroups, true, {INTEGER()}, BIGINT(),
                                               {1, 2, 3, 4, 5, std::nullopt});
    checkPartialSum<int64_t, int64_t, int64_t>(numGroups, true, {BIGINT()}, BIGINT(),
                                               {1, 2, 3, 4, 5, std::nullopt});
    checkPartialSum<float, double, float>(numGroups, true, {REAL()}, DOUBLE(),
                                          {1, 2, 3, 4, 5, std::nullopt});
    checkPartialSum<double, double, double>(numGroups, true, {DOUBLE()}, DOUBLE(),
                                            {1, 2, 3, 4, 5, std::nullopt});
  }
}

TEST_F(TrinoSumAggregateTest, final) {
  for (int numGroups = 1; numGroups <= 5; ++numGroups) {
    checkFinalSum<int64_t, int64_t, int64_t>(numGroups, false,
                                             {ROW({BIGINT(), BIGINT()})}, BIGINT(),
                                             {10, 20, 30, 40, 50}, {1, 2, 3, 4, 5});
    checkFinalSum<float, double, float>(numGroups, false, {ROW({REAL(), BIGINT()})},
                                        REAL(), {10, 20, 30, 40, 50}, {1, 2, 3, 4, 5});
    checkFinalSum<double, double, double>(numGroups, false, {ROW({DOUBLE(), BIGINT()})},
                                          DOUBLE(), {10, 20, 30, 40, 50},
                                          {1, 2, 3, 4, 5});
  }
}

TEST_F(TrinoSumAggregateTest, finalWithNull) {
  for (int numGroups = 1; numGroups <= 5; ++numGroups) {
    checkFinalSum<int64_t, int64_t, int64_t>(
        numGroups, false, {ROW({BIGINT(), BIGINT()})}, BIGINT(),
        {10, 20, 30, 40, 50, std::nullopt}, {1, 2, 3, 4, 5, std::nullopt});
    checkFinalSum<float, double, float>(numGroups, false, {ROW({REAL(), BIGINT()})},
                                        REAL(), {10, 20, 30, 40, 50, std::nullopt},
                                        {1, 2, 3, 4, 5, std::nullopt});
    checkFinalSum<double, double, double>(numGroups, false, {ROW({DOUBLE(), BIGINT()})},
                                          DOUBLE(), {10, 20, 30, 40, 50, std::nullopt},
                                          {1, 2, 3, 4, 5, std::nullopt});
  }
}

TEST_F(TrinoSumAggregateTest, finalWithConstant) {
  for (int numGroups = 1; numGroups <= 5; ++numGroups) {
    checkFinalSum<int64_t, int64_t, int64_t>(numGroups, true, {ROW({BIGINT(), BIGINT()})},
                                             BIGINT(), {10, 20, 30, 40, 50, std::nullopt},
                                             {1, 2, 3, 4, 5, std::nullopt});
    checkFinalSum<float, double, float>(numGroups, true, {ROW({REAL(), BIGINT()})},
                                        REAL(), {10, 20, 30, 40, 50, std::nullopt},
                                        {1, 2, 3, 4, 5, std::nullopt});
    checkFinalSum<double, double, double>(numGroups, true, {ROW({DOUBLE(), BIGINT()})},
                                          DOUBLE(), {10, 20, 30, 40, 50, std::nullopt},
                                          {1, 2, 3, 4, 5, std::nullopt});
  }
}
