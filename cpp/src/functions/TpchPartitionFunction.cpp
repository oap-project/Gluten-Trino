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
#include "TpchPartitionFunction.h"

namespace io::trino::bridge {

TpchPartitionFunction::TpchPartitionFunction(int64_t rowsPerBucket,
                                             std::vector<int32_t> bucketToPartition)
    : _rowsPerBucket(rowsPerBucket),
      _bucketCount(bucketToPartition.size()),
      _bucketToPartition(bucketToPartition) {}

std::optional<uint32_t> TpchPartitionFunction::partition(
    const facebook::velox::RowVector& input, std::vector<uint32_t>& partitions) {
  auto size = input.size();
  partitions.resize(size);

  const auto& block = input.childAt(0);
  decodedVector.decode(*block);
  for (auto i = 0; i < size; ++i) {
    if (block->isNullAt(i)) {
      partitions[i] = 0;
      continue;
    }

    auto orderKey = decodedVector.valueAt<int64_t>(i);
    int64_t rowNumber = rowNumberFromOrderKey(orderKey);
    int64_t bucket = rowNumber / _rowsPerBucket;
    VELOX_CHECK_EQ(static_cast<int32_t>(bucket), bucket, "integer overflow");

    if (bucket >= _bucketCount) {
      bucket = _bucketCount - 1;
    }
    partitions[i] = _bucketToPartition[static_cast<uint32_t>(bucket)];
  }

  return std::nullopt;
}

int64_t TpchPartitionFunction::rowNumberFromOrderKey(int64_t orderKey) {
  return (((orderKey & ~(0b11'111)) >> 2) | orderKey & 0b111) - 1;
}

TpchPartitionFunctionSpec::TpchPartitionFunctionSpec(
    int64_t rowsPerBucket, std::vector<int32_t> bucketToPartition)
    : _rowsPerBucket(rowsPerBucket), _bucketToPartition(bucketToPartition) {}

std::string TpchPartitionFunctionSpec::toString() const { return "TPCH"; }

std::unique_ptr<core::PartitionFunction> TpchPartitionFunctionSpec::create(
    int /* numPartitions */) const {
  return std::make_unique<TpchPartitionFunction>(_rowsPerBucket, _bucketToPartition);
}

folly::dynamic TpchPartitionFunctionSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = fmt::format("TpchPartitionFunctionSpec");
  obj["rowsPerBucket"] = ISerializable::serialize(_rowsPerBucket);
  obj["bucketToPartition"] = ISerializable::serialize(_bucketToPartition);
  return obj;
}

core::PartitionFunctionSpecPtr TpchPartitionFunctionSpec::deserialize(
    const folly::dynamic& obj, void* context) {
  auto rowsPerBucket = ISerializable::deserialize<int64_t>(obj["rowsPerBucket"], context);
  auto bucketToPartition =
      ISerializable::deserialize<std::vector<int>>(obj["bucketToPartition"], context);
  return std::make_shared<TpchPartitionFunctionSpec>(rowsPerBucket, bucketToPartition);
}

}  // namespace io::trino::bridge
