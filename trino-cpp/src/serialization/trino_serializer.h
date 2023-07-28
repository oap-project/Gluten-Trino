#pragma once
#include "velox/common/base/Crc.h"
#include "velox/vector/VectorStream.h"

namespace io::trino::bridge {

class TrinoVectorSerde : public facebook::velox::VectorSerde {
 public:
  TrinoVectorSerde() {}
  struct TrinoOptions : VectorSerde::Options {
    explicit TrinoOptions(bool useLosslessTimestamp)
        : useLosslessTimestamp(useLosslessTimestamp) {}
    // Currently presto only supports millisecond precision and the serializer
    // converts velox native timestamp to that resulting in loss of precision.
    // This option allows it to serialize with nanosecond precision and is
    // currently used for spilling. Is false by default.
    bool useLosslessTimestamp{false};
  };

  void estimateSerializedSize(
      facebook::velox::VectorPtr vector,
      const folly::Range<const facebook::velox::IndexRange*>& ranges,
      facebook::velox::vector_size_t** sizes) override;

  std::unique_ptr<facebook::velox::VectorSerializer> createSerializer(
      std::shared_ptr<const facebook::velox::RowType> type, int32_t numRows,
      facebook::velox::StreamArena* streamArena, const Options* options) override;

  void deserialize(facebook::velox::ByteStream* source,
                   facebook::velox::memory::MemoryPool* pool,
                   facebook::velox::RowTypePtr type,
                   facebook::velox::RowVectorPtr* result,
                   const Options* options = nullptr) override;
  static void registerVectorSerde();
};

class TrinoOutputStreamListener : public facebook::velox::OutputStreamListener {};
}  // namespace io::trino::bridge