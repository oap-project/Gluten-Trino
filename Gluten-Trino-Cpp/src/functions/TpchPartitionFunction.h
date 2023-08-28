#pragma once

#include "velox/core/PlanNode.h"

using namespace facebook::velox;

namespace io::trino::bridge {

class TpchPartitionFunction : public core::PartitionFunction {
 public:
  explicit TpchPartitionFunction(int64_t rowsPerPartition, int numPartitions);

  std::optional<uint32_t> partition(const RowVector& input, std::vector<uint32_t>& partitions);

 private:
  int64_t rowNumberFromOrderKey(int64_t orderKey);

 private:
  const int64_t _rowsPerPartition;
  const int _numPartitions;

  DecodedVector decodedVector;
};

class TpchPartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  explicit TpchPartitionFunctionSpec(int64_t rowsPerPartition);

  std::unique_ptr<core::PartitionFunction> create(int numPartitions) const override;

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static core::PartitionFunctionSpecPtr deserialize(const folly::dynamic& obj,
                                                    void* context);

 private:
  const int64_t _rowsPerPartition;
};

}  // namespace io::trino::bridge