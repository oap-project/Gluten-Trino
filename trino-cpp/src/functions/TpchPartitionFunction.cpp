#include "TpchPartitionFunction.h"

namespace io::trino::bridge {

TpchPartitionFunction::TpchPartitionFunction(int64_t rowsPerPartition, int numPartitions)
    : _rowsPerPartition(rowsPerPartition), _numPartitions(numPartitions) {}

void TpchPartitionFunction::partition(const facebook::velox::RowVector& input,
                                      std::vector<uint32_t>& partitions) {
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
    int64_t bucket = rowNumber / _rowsPerPartition;

    VELOX_CHECK_EQ(static_cast<int32_t>(bucket), bucket, "integer overflow");

    if (bucket >= _numPartitions) {
      bucket = _numPartitions - 1;
    }
    partitions[i] = static_cast<uint32_t>(bucket);
  }
}

int64_t TpchPartitionFunction::rowNumberFromOrderKey(int64_t orderKey) {
  return (((orderKey & ~(0b11111)) >> 2) | orderKey & 0b111) - 1;
}

TpchPartitionFunctionSpec::TpchPartitionFunctionSpec(int64_t rowsPerPartition)
    : _rowsPerPartition(rowsPerPartition) {}

std::string TpchPartitionFunctionSpec::toString() const { return "TPCH"; }

std::unique_ptr<core::PartitionFunction> TpchPartitionFunctionSpec::create(
    int numPartitions) const {
  return std::make_unique<TpchPartitionFunction>(_rowsPerPartition, numPartitions);
}

folly::dynamic TpchPartitionFunctionSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = fmt::format("TpchPartitionFunctionSpec");
  obj["rowsPerPartition"] = ISerializable::serialize(_rowsPerPartition);
  return obj;
}

core::PartitionFunctionSpecPtr TpchPartitionFunctionSpec::deserialize(
    const folly::dynamic& obj, void* context) {
  auto rowsPerPartition =
      ISerializable::deserialize<int64_t>(obj["rowsPerPartition"], context);
  return std::make_shared<TpchPartitionFunctionSpec>(rowsPerPartition);
}

}  // namespace io::trino::bridge
