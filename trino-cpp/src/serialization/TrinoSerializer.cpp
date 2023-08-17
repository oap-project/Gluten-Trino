#include "TrinoSerializer.h"
#include <glog/logging.h>
#include "velox/common/base/Crc.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/BiasVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

#include <iostream>

#include "TrinoByteStream.h"
#include "utils/JniUtils.h"

using namespace facebook::velox;

namespace io::trino::bridge {

constexpr folly::StringPiece kRLE{"RLE"};

std::string typeToEncodingName(const TypePtr& type) {
  if (type->isDate()) {
    return "INT_ARRAY";
  }
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return "BYTE_ARRAY";
    case TypeKind::TINYINT:
      return "BYTE_ARRAY";
    case TypeKind::SMALLINT:
      return "SHORT_ARRAY";
    case TypeKind::INTEGER:
      return "INT_ARRAY";
    case TypeKind::BIGINT:
      return "LONG_ARRAY";
    case TypeKind::REAL:
      return "INT_ARRAY";
    case TypeKind::DOUBLE:
      return "LONG_ARRAY";
    case TypeKind::VARCHAR:
      return "VARIABLE_WIDTH";
    case TypeKind::VARBINARY:
      return "VARIABLE_WIDTH";
    case TypeKind::TIMESTAMP:
      return "LONG_ARRAY";
    case TypeKind::HUGEINT:
      return "INT128_ARRAY";
    case TypeKind::ARRAY:
      return "ARRAY";
    case TypeKind::MAP:
      return "MAP";
    case TypeKind::ROW:
      return isTimestampWithTimeZoneType(type) ? "LONG_ARRAY" : "ROW";
    case TypeKind::UNKNOWN:
      return "BYTE_ARRAY";
    default:
      throw std::runtime_error("Unknown type kind");
  }
}
void writeInt32(OutputStream* out, int32_t value) {
  out->write(reinterpret_cast<char*>(&value), sizeof(value));
}

void writeInt64(OutputStream* out, int64_t value) {
  out->write(reinterpret_cast<char*>(&value), sizeof(value));
}

class CountingOutputStream : public OutputStream {
 public:
  explicit CountingOutputStream() : OutputStream{nullptr} {}

  void write(const char* /*s*/, std::streamsize count) override {
    pos_ += count;
    if (numBytes_ < pos_) {
      numBytes_ = pos_;
    }
  }

  std::streampos tellp() const override { return pos_; }

  void seekp(std::streampos pos) override { pos_ = pos; }

  std::streamsize size() const { return numBytes_; }

 private:
  std::streamsize numBytes_{0};
  std::streampos pos_{0};
};

class VectorStream {
 public:
  VectorStream(const TypePtr type, StreamArena* streamArena, int32_t initialNumRows,
               bool useLosslessTimestamp)
      : type_(type),
        useLosslessTimestamp_(useLosslessTimestamp),
        nulls_(streamArena, true, true),
        lengths_(streamArena),
        offset_(streamArena),
        values_(streamArena) {
    streamArena->newTinyRange(50, &header_);
    auto name = typeToEncodingName(type);
    header_.size = name.size() + sizeof(int32_t);
    *reinterpret_cast<int32_t*>(header_.buffer) = name.size();
    memcpy(header_.buffer + sizeof(int32_t), &name[0], name.size());
    nulls_.startWrite(1 + (initialNumRows / 8));
    if (initialNumRows > 0) {
      switch (type_->kind()) {
        case TypeKind::ROW:
          hasOffset_ = true;
          offset_.startWrite((initialNumRows + 1) * sizeof(vector_size_t));
          if (isTimestampWithTimeZoneType(type_)) {
            values_.startWrite(initialNumRows * 4);
            break;
          }  // else fall through
        case TypeKind::ARRAY:
        case TypeKind::MAP:
          hasLengths_ = true;
          lengths_.startWrite(initialNumRows * sizeof(vector_size_t));
          children_.resize(type_->size());
          for (int32_t i = 0; i < type_->size(); ++i) {
            children_[i] = std::make_unique<VectorStream>(
                type_->childAt(i), streamArena, initialNumRows, useLosslessTimestamp);
          }
          break;
        case TypeKind::VARCHAR:
        case TypeKind::VARBINARY:
          hasLengths_ = true;
          lengths_.startWrite(initialNumRows * sizeof(vector_size_t));
          values_.startWrite(initialNumRows * 10);
          break;

        default:;
          values_.startWrite(initialNumRows * 4);
          break;
      }
    }
  }

  void appendNull() {
    if (nonNullCount_ && nullCount_ == 0) {
      nulls_.appendBool(false, nonNullCount_);
    }
    nulls_.appendBool(true, 1);
    ++nullCount_;
    if (hasOffset_) {
      appendOffset(0);
    }
  }

  void appendNonNull(int32_t count = 1) {
    if (nullCount_) {
      nulls_.appendBool(false, count);
    }
    nonNullCount_ += count;
  }

  void appendLength(int32_t length) {
    if (nullCount_ + nonNullCount_ == 1) {
      // The first element in the offsets in the wire format is always 0 for
      // nested types but not for string.
      auto kind = type_->kind();
      if (kind == TypeKind::ROW || kind == TypeKind::ARRAY || kind == TypeKind::MAP) {
        lengths_.appendOne<int32_t>(0);
      }
    }
    // totalLength_ += length;
    lengths_.appendOne<int32_t>(length);
  }

  // for row type
  void appendOffset(int32_t length) {
    if (nullCount_ + nonNullCount_ == 1) {
      // The first element in the offsets in the wire format is always 0 for
      // nested types but not for string.
      auto kind = type_->kind();
      if (kind == TypeKind::ROW) {
        offset_.appendOne<int32_t>(0);
      }
    }
    offsetValue += length;
    offset_.appendOne<int32_t>(offsetValue);
  }

  template <typename T>
  void append(folly::Range<const T*> values) {
    values_.append(values);
  }

  template <typename T>
  void appendOne(const T& value) {
    append(folly::Range(&value, 1));
  }

  VectorStream* childAt(int32_t index) { return children_[index].get(); }

  // Returns the size to flush to OutputStream before calling `flush`.
  size_t serializedSize() {
    CountingOutputStream out;
    flush(&out);
    return out.size();
  }

  // Writes out the accumulated contents. Does not change the state.
  void flush(OutputStream* out) {
    out->write(reinterpret_cast<char*>(header_.buffer), header_.size);
    switch (type_->kind()) {
      case TypeKind::ROW:
        if (isTimestampWithTimeZoneType(type_)) {
          writeInt32(out, nullCount_ + nonNullCount_);
          flushNulls(out);
          values_.flush(out);
          return;
        }

        writeInt32(out, children_.size());
        writeInt32(out, nullCount_ + nonNullCount_);  // diff with presto:
        for (auto& child : children_) {
          child->flush(out);
        }

        flushNulls(out);

        if (nullCount_ + nonNullCount_ == 0) {
          // If nothing was added, there is still one offset in the wire format.
          offset_.appendOne<int32_t>(0);
        }
        if (nullCount_) {
          offset_.flush(out);
        }

        return;

      case TypeKind::ARRAY:
        children_[0]->flush(out);
        writeInt32(out, nullCount_ + nonNullCount_);
        if (nullCount_ + nonNullCount_ == 0) {
          // If nothing was added, there is still one offset in the wire format.
          lengths_.appendOne<int32_t>(0);
        }
        lengths_.flush(out);
        flushNulls(out);
        return;

      case TypeKind::MAP: {
        children_[0]->flush(out);
        children_[1]->flush(out);
        // hash table size. -1 means not included in serialization.
        writeInt32(out, -1);
        writeInt32(out, nullCount_ + nonNullCount_);
        if (nullCount_ + nonNullCount_ == 0) {
          // If nothing was added, there is still one offset in the wire format.
          offset_.appendOne<int32_t>(0);
        }

        offset_.flush(out);
        flushNulls(out);
        return;
      }

      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        writeInt32(out, nullCount_ + nonNullCount_);
        writeInt32(out, nonNullCount_);
        lengths_.flush(out);
        flushNulls(out);
        writeInt32(out, values_.size());
        values_.flush(out);
        return;

      default:
        writeInt32(out, nullCount_ + nonNullCount_);
        flushNulls(out);
        if (nullCount_) {
          writeInt32(out, nonNullCount_);
        }

        values_.flush(out);
    }
  }

  void flushNulls(OutputStream* out) {
    if (!nullCount_) {
      char zero = 0;
      out->write(&zero, 1);
    } else {
      char one = 1;
      out->write(&one, 1);
      nulls_.flush(out);
    }
  }

 private:
  const TypePtr type_;
  /// Indicates whether to serialize timestamps with nanosecond precision.
  /// If false, they are serialized with millisecond precision which is
  /// compatible with presto.
  const bool useLosslessTimestamp_;
  int32_t nonNullCount_{0};
  int32_t nullCount_{0};
  int32_t totalLength_{0};
  bool hasLengths_{false};
  bool hasOffset_{false};
  ByteRange header_;
  // ByteStream nulls_;
  TrinoNullByteStream nulls_;
  ByteStream lengths_;
  ByteStream offset_;  // For Row Type.
  int32_t offsetValue{0};
  ByteStream values_;
  std::vector<std::unique_ptr<VectorStream>> children_;
};

template <>
inline void VectorStream::append(folly::Range<const StringView*> values) {
  for (auto& value : values) {
    auto size = value.size();
    appendLength(size);
    values_.appendStringPiece(folly::StringPiece(value.data(), size));
  }
}

template <>
void VectorStream::append(folly::Range<const Timestamp*> values) {
  if (useLosslessTimestamp_) {
    for (auto& value : values) {
      appendOne(value.toNanos());
    }
  } else {
    for (auto& value : values) {
      appendOne(value.toMillis());
    }
  }
}

template <>
void VectorStream::append(folly::Range<const bool*> values) {
  // A bool constant is serialized via this. Accessing consecutive
  // elements via bool& does not work, hence the flat serialization is
  // specialized one level above this.
  VELOX_CHECK(values.size() == 1);
  appendOne<uint8_t>(values[0] ? 1 : 0);
}

FOLLY_ALWAYS_INLINE int128_t toJavaDecimalValue(int128_t value) {
  constexpr int128_t kInt128SerializeMask = (static_cast<int128_t>(1) << 127);
  // We need to reverse the high and low because reading original lower part bytes in
  // Java side of Trino is earlier than reading original higher part.
  uint64_t high = static_cast<uint64_t>(value & 0xffffffffffffffff);
  uint64_t low = static_cast<uint64_t>((value >> 64) & 0xffffffffffffffff);
  value = (static_cast<int128_t>(high) << 64) | static_cast<int128_t>(low);
  // Presto Java UnscaledDecimal128 representation uses signed magnitude
  // representation. Only negative values differ in this representation.
  if (value < 0) {
    value *= -1;
    value |= kInt128SerializeMask;
  }
  return value;
}

template <>
void VectorStream::append(folly::Range<const int128_t*> values) {
  for (auto& value : values) {
    int128_t val = value;
    if (type_->isLongDecimal()) {
      val = toJavaDecimalValue(value);
    }
    values_.append<int128_t>(folly::Range(&val, 1));
  }
}

void serializeColumn(const BaseVector* vector,
                     const folly::Range<const IndexRange*>& ranges, VectorStream* stream);

void serializeWrapped(const BaseVector* vector,
                      const folly::Range<const IndexRange*>& ranges,
                      VectorStream* stream) {
  std::vector<IndexRange> newRanges;
  bool mayHaveNulls = vector->mayHaveNulls();
  const BaseVector* wrapped = vector->wrappedVector();
  for (int32_t i = 0; i < ranges.size(); ++i) {
    auto end = ranges[i].begin + ranges[i].size;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (mayHaveNulls && vector->isNullAt(offset)) {
        // The wrapper added a null.
        if (!newRanges.empty()) {
          serializeColumn(wrapped, newRanges, stream);
          newRanges.clear();
        }
        stream->appendNull();
        continue;
      }
      auto innerIndex = vector->wrappedIndex(offset);
      newRanges.push_back(IndexRange{innerIndex, 1});
    }
  }
  if (!newRanges.empty()) {
    serializeColumn(wrapped, newRanges, stream);
  }
}

static inline int32_t rangesTotalSize(const folly::Range<const IndexRange*>& ranges) {
  int32_t total = 0;
  for (auto& range : ranges) {
    total += range.size;
  }
  return total;
}

template <TypeKind kind>
void serializeFlatVector(const BaseVector* vector,
                         const folly::Range<const IndexRange*>& ranges,
                         VectorStream* stream) {
  using T = typename TypeTraits<kind>::NativeType;
  auto flatVector = dynamic_cast<const FlatVector<T>*>(vector);
  auto rawValues = flatVector->rawValues();
  if (!flatVector->mayHaveNulls()) {
    for (auto& range : ranges) {
      stream->appendNonNull(range.size);
      stream->append<T>(folly::Range(&rawValues[range.begin], range.size));
    }
  } else {
    int32_t firstNonNull = -1;
    int32_t lastNonNull = -1;
    for (int32_t i = 0; i < ranges.size(); ++i) {
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (flatVector->isNullAt(offset)) {
          stream->appendNull();
          continue;
        }
        stream->appendNonNull();
        if (std::is_same_v<T, StringView>) {
          // Bunching consecutive non-nulls into one append does not work with
          // strings because the lengths will then get out of order with the
          // zero lengths produced by nulls.
          stream->appendOne(rawValues[offset]);
        } else if (firstNonNull == -1) {
          firstNonNull = offset;
          lastNonNull = offset;
        } else if (offset == lastNonNull + 1) {
          lastNonNull = offset;
        } else {
          stream->append<T>(
              folly::Range(&rawValues[firstNonNull], 1 + lastNonNull - firstNonNull));
          firstNonNull = offset;
          lastNonNull = offset;
        }
      }
    }
    if (firstNonNull != -1 && !std::is_same_v<T, StringView>) {
      stream->append<T>(
          folly::Range(&rawValues[firstNonNull], 1 + lastNonNull - firstNonNull));
    }
  }
}

template <>
void serializeFlatVector<TypeKind::BOOLEAN>(const BaseVector* vector,
                                            const folly::Range<const IndexRange*>& ranges,
                                            VectorStream* stream) {
  auto flatVector = dynamic_cast<const FlatVector<bool>*>(vector);
  if (!vector->mayHaveNulls()) {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      stream->appendNonNull(ranges[i].size);
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        stream->appendOne<uint8_t>(flatVector->valueAtFast(offset) ? 1 : 0);
      }
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (vector->isNullAt(offset)) {
          stream->appendNull();
          continue;
        }
        stream->appendNonNull();
        stream->appendOne<uint8_t>(flatVector->valueAtFast(offset) ? 1 : 0);
      }
    }
  }
}

template <TypeKind kind>
void serializeConstantVector(const BaseVector* vector,
                             const folly::Range<const IndexRange*>& ranges,
                             VectorStream* stream) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  auto constVector = dynamic_cast<const ConstantVector<T>*>(vector);
  if (constVector->valueVector()) {
    serializeWrapped(constVector, ranges, stream);
    return;
  }
  int32_t count = rangesTotalSize(ranges);
  if (vector->isNullAt(0)) {
    for (int32_t i = 0; i < count; ++i) {
      stream->appendNull();
    }
    return;
  }

  T value = constVector->valueAtFast(0);
  for (int32_t i = 0; i < count; ++i) {
    stream->appendNonNull();
    stream->appendOne(value);
  }
}

template <typename T>
void serializeBiasVector(const BaseVector* vector,
                         const folly::Range<const IndexRange*>& ranges,
                         VectorStream* stream) {
  auto biasVector = dynamic_cast<const BiasVector<T>*>(vector);
  if (!vector->mayHaveNulls()) {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      stream->appendNonNull(ranges[i].size);
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        stream->appendOne(biasVector->valueAtFast(offset));
      }
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (biasVector->isNullAt(offset)) {
          stream->appendNull();
          continue;
        }
        stream->appendNonNull();
        stream->appendOne(biasVector->valueAtFast(offset));
      }
    }
  }
}

int64_t packTimestampWithTimeZone(int64_t timestamp, int16_t timezone) {
  return timezone | (timestamp << 12);
}

void unpackTimestampWithTimeZone(int64_t packed, int64_t& timestamp, int16_t& timezone) {
  timestamp = packed >> 12;
  timezone = packed & 0xfff;
}

void serializeTimestampWithTimeZone(const RowVector* rowVector,
                                    const folly::Range<const IndexRange*>& ranges,
                                    VectorStream* stream) {
  auto timestamps = rowVector->childAt(0)->as<SimpleVector<int64_t>>();
  auto timezones = rowVector->childAt(1)->as<SimpleVector<int16_t>>();
  for (const auto& range : ranges) {
    for (auto i = range.begin; i < range.begin + range.size; ++i) {
      if (rowVector->isNullAt(i)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        stream->appendOne(
            packTimestampWithTimeZone(timestamps->valueAt(i), timezones->valueAt(i)));
      }
    }
  }
}

void serializeRowVector(const BaseVector* vector,
                        const folly::Range<const IndexRange*>& ranges,
                        VectorStream* stream) {
  auto rowVector = dynamic_cast<const RowVector*>(vector);

  if (isTimestampWithTimeZoneType(vector->type())) {
    serializeTimestampWithTimeZone(rowVector, ranges, stream);
    return;
  }

  std::vector<IndexRange> childRanges;
  for (int32_t i = 0; i < ranges.size(); ++i) {
    auto begin = ranges[i].begin;
    auto end = begin + ranges[i].size;
    for (auto offset = begin; offset < end; ++offset) {
      if (rowVector->isNullAt(offset)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        stream->appendOffset(1);
        childRanges.push_back(IndexRange{offset, 1});
      }
    }
  }
  for (int32_t i = 0; i < rowVector->childrenSize(); ++i) {
    serializeColumn(rowVector->childAt(i).get(), childRanges, stream->childAt(i));
  }
}

void serializeArrayVector(const BaseVector* vector,
                          const folly::Range<const IndexRange*>& ranges,
                          VectorStream* stream) {
  auto arrayVector = dynamic_cast<const ArrayVector*>(vector);
  auto rawSizes = arrayVector->rawSizes();
  auto rawOffsets = arrayVector->rawOffsets();
  std::vector<IndexRange> childRanges;
  childRanges.reserve(ranges.size());
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t begin = ranges[i].begin;
    int32_t end = begin + ranges[i].size;
    for (int32_t offset = begin; offset < end; ++offset) {
      if (arrayVector->isNullAt(offset)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        auto size = rawSizes[offset];
        stream->appendLength(size);  // To be fixed.
        if (size > 0) {
          childRanges.emplace_back<IndexRange>({rawOffsets[offset], size});
        }
      }
    }
  }
  serializeColumn(arrayVector->elements().get(), childRanges, stream->childAt(0));
}

void serializeMapVector(const BaseVector* vector,
                        const folly::Range<const IndexRange*>& ranges,
                        VectorStream* stream) {
  auto mapVector = dynamic_cast<const MapVector*>(vector);
  auto rawSizes = mapVector->rawSizes();
  auto rawOffsets = mapVector->rawOffsets();
  std::vector<IndexRange> childRanges;
  childRanges.reserve(ranges.size());
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t begin = ranges[i].begin;
    int32_t end = begin + ranges[i].size;
    for (int32_t offset = begin; offset < end; ++offset) {
      if (mapVector->isNullAt(offset)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        auto size = rawSizes[offset];
        stream->appendLength(size);  // To be fixed.
        if (size > 0) {
          childRanges.emplace_back<IndexRange>({rawOffsets[offset], size});
        }
      }
    }
  }
  serializeColumn(mapVector->mapKeys().get(), childRanges, stream->childAt(0));
  serializeColumn(mapVector->mapValues().get(), childRanges, stream->childAt(1));
}

void serializeColumn(const BaseVector* vector,
                     const folly::Range<const IndexRange*>& ranges,
                     VectorStream* stream) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(serializeFlatVector, vector->typeKind(),
                                             vector, ranges, stream);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(serializeConstantVector, vector->typeKind(), vector,
                                      ranges, stream);
      break;
    case VectorEncoding::Simple::BIASED:
      switch (vector->typeKind()) {
        case TypeKind::SMALLINT:
          serializeBiasVector<int16_t>(vector, ranges, stream);
          break;
        case TypeKind::INTEGER:
          serializeBiasVector<int32_t>(vector, ranges, stream);
          break;
        case TypeKind::BIGINT:
          serializeBiasVector<int64_t>(vector, ranges, stream);
          break;
        default:
          throw std::invalid_argument("Invalid biased vector type");
      }
      break;
    case VectorEncoding::Simple::ROW:
      serializeRowVector(vector, ranges, stream);
      break;
    case VectorEncoding::Simple::ARRAY:
      serializeArrayVector(vector, ranges, stream);
      break;
    case VectorEncoding::Simple::MAP:
      serializeMapVector(vector, ranges, stream);
      break;
    default:
      serializeWrapped(vector, ranges, stream);
  }
}

class TrinoVectorSerializer : public facebook::velox::VectorSerializer {
 public:
  TrinoVectorSerializer(std::shared_ptr<const RowType> rowType, int32_t numRows,
                        StreamArena* streamArena, bool useLosslessTimestamp) {
    auto types = rowType->children();
    auto numTypes = types.size();
    streams_.resize(numTypes);
    for (int i = 0; i < numTypes; i++) {
      streams_[i] = std::make_unique<VectorStream>(types[i], streamArena, numRows,
                                                   useLosslessTimestamp);
    }
  }

  void append(const facebook::velox::RowVectorPtr& vector,
              const folly::Range<const IndexRange*>& ranges) override {
    auto newRows = rangesTotalSize(ranges);
    if (newRows > 0) {
      numRows_ += newRows;
      for (int32_t i = 0; i < vector->childrenSize(); ++i) {
        serializeColumn(vector->childAt(i).get(), ranges, streams_[i].get());
      }
    }
  }

  size_t maxSerializedSize() const override {
    size_t dataSize = 4;  // streams_.size()
    for (auto& stream : streams_) {
      dataSize += stream->serializedSize();
    }

    // auto compressedSize = needCompression(*codec_)
    //                           ? codec_->maxCompressedLength(dataSize)
    //                           : dataSize;
    return kHeaderSize + dataSize;
  }

  void flush(OutputStream* out) override { flushInternal(numRows_, false /*rle*/, out); }

  void flushRle(const RowVectorPtr& vector, OutputStream* out) {
    VELOX_CHECK_EQ(0, numRows_);
    for (auto& child : vector->children()) {
      VELOX_CHECK(child->isConstantEncoding());
    }

    std::vector<IndexRange> ranges{{0, 1}};
    append(vector, folly::Range(ranges.data(), ranges.size()));

    flushInternal(vector->size(), true /*rle*/, out);
  }

  // Writes the contents to 'stream' in wire format
  void flushInternal(int32_t numRows, bool rle, OutputStream* out) {
    char codec = 0;
    // if (listener) {
    //   codec = getCodecMarker();
    // }

    int32_t offset = out->tellp();

    writeInt32(out, numRows);
    out->write(&codec, 1);

    // Make space for uncompressedSizeInBytes & sizeInBytes
    writeInt32(out, 0);
    writeInt32(out, 0);

    writeInt32(out, streams_.size());

    if (rle) {
      // Write RLE encoding marker.
      writeInt32(out, kRLE.size());
      out->write(kRLE.data(), kRLE.size());
      // Write number of RLE values.
      writeInt32(out, numRows);
    }

    for (auto& stream : streams_) {
      stream->flush(out);
    }

    // Fill in uncompressedSizeInBytes & sizeInBytes
    int32_t size = (int32_t)out->tellp() - offset;
    int32_t uncompressedSize = size - kHeaderSize;

    out->seekp(offset + kSizeInBytesOffset);
    writeInt32(out, uncompressedSize);
    writeInt32(out, uncompressedSize);

    out->seekp(offset + size);
  }

 private:
  static const int32_t kSizeInBytesOffset{4 + 1};  // row_count + codec
  static const int32_t kHeaderSize{kSizeInBytesOffset + 4 +
                                   4};  // uncompressed_size + compressed_size

  int32_t numRows_{0};
  std::vector<std::unique_ptr<VectorStream>> streams_;
};

std::unique_ptr<VectorSerializer> TrinoVectorSerde::createSerializer(
    std::shared_ptr<const RowType> type, int32_t numRows, StreamArena* streamArena,
    const Options* options) {
  bool useLosslessTimestamp =
      options != nullptr ? static_cast<const TrinoOptions*>(options)->useLosslessTimestamp
                         : false;
  return std::make_unique<TrinoVectorSerializer>(type, numRows, streamArena,
                                                 useLosslessTimestamp);
}

void readColumns(ByteStream* source, memory::MemoryPool* pool,
                 const std::vector<TypePtr>& types, std::vector<VectorPtr>* result,
                 bool useLosslessTimestamp);

// if the column is not nullable, returns -1.
// if the column is nullable, but null count is 0, return 0.
vector_size_t readNulls(ByteStream* source, vector_size_t size, BaseVector* result) {
  if (source->readByte() == 0) {
    result->clearNulls(0, size);
    result->setNullCount(0);
    return -1;  // means the column is not nullable.
  }

  BufferPtr nulls = result->mutableNulls(size);
  auto rawNulls = nulls->asMutable<uint8_t>();
  auto numBytes = BaseVector::byteSize<bool>(size);

  source->readBytes(rawNulls, numBytes);
  bits::reverseBits(rawNulls, numBytes);
  bits::negate(reinterpret_cast<char*>(rawNulls), numBytes * 8);
  vector_size_t nullCount = nulls ? BaseVector::countNulls(nulls, 0, size) : 0;
  result->setNullCount(nullCount);
  // null count could be 0
  return nullCount;
}

template <typename T>
void readValues(ByteStream* source, vector_size_t size, BufferPtr nulls,
                vector_size_t nullCount, BufferPtr values) {
  if (nullCount != -1) {
    // Reference:
    //   io.trino.spi.block.LongArrayBlockEncoding#readBlock
    //   io.trino.spi.block.IntArrayBlockEncoding#readBlock
    int32_t nonNullPositionCount = source->read<int32_t>();
    VLOG(1) << "Read nonNullPositionCount is " << nonNullPositionCount;
    if (nonNullPositionCount) {
      auto rawValues = values->asMutable<T>();
      int32_t sliceSize = Unsafe::instance().arraySliceSize<T>(nonNullPositionCount);
      source->readBytes(reinterpret_cast<uint8_t*>(rawValues), sliceSize);

      int position = nonNullPositionCount - 1;
      auto rawNulls = nulls->as<uint64_t>();
      for (int i = size - 1; i >= 0; i--) {
        rawValues[i] = rawValues[position];
        if (!bits::isBitNull(rawNulls, i)) {
          position--;
        }
      }
    }
  } else {
    int32_t sliceSize = Unsafe::instance().arraySliceSize<T>(size);
    if (sliceSize) {
      source->readBytes(values->asMutable<uint8_t>(), sliceSize);
    }
  }
}

template <>
void readValues<bool>(ByteStream* source, vector_size_t size, BufferPtr nulls,
                      vector_size_t nullCount, BufferPtr values) {
  auto rawValues = values->asMutable<uint64_t>();
  if (nullCount != -1) {
    int32_t toClear = 0;
    bits::forEachSetBit(nulls->as<uint64_t>(), 0, size, [&](int32_t row) {
      // Set the values between the last non-null and this to type default.
      for (; toClear < row; ++toClear) {
        bits::clearBit(rawValues, toClear);
      }
      bits::setBit(rawValues, row, (source->read<int8_t>() != 0));
      toClear = row + 1;
    });
  } else {
    for (int32_t row = 0; row < size; ++row) {
      bits::setBit(rawValues, row, (source->read<int8_t>() != 0));
    }
  }
}

Timestamp readTimestamp(ByteStream* source) {
  int64_t millis = source->read<int64_t>();
  return Timestamp::fromMillis(millis);
}

template <>
void readValues<Timestamp>(ByteStream* source, vector_size_t size, BufferPtr nulls,
                           vector_size_t nullCount, BufferPtr values) {
  auto rawValues = values->asMutable<Timestamp>();
  if (nullCount != -1) {
    int32_t toClear = 0;
    bits::forEachSetBit(nulls->as<uint64_t>(), 0, size, [&](int32_t row) {
      // Set the values between the last non-null and this to type default.
      for (; toClear < row; ++toClear) {
        rawValues[toClear] = Timestamp();
      }
      rawValues[row] = readTimestamp(source);
      toClear = row + 1;
    });
  } else {
    for (int32_t row = 0; row < size; ++row) {
      rawValues[row] = readTimestamp(source);
    }
  }
}

Timestamp readLosslessTimestamp(ByteStream* source) {
  int64_t nanos = source->read<int64_t>();
  return Timestamp::fromNanos(nanos);
}

void readLosslessTimestampValues(ByteStream* source, vector_size_t size, BufferPtr nulls,
                                 vector_size_t nullCount, BufferPtr values) {
  auto rawValues = values->asMutable<Timestamp>();
  if (nullCount > 0) {
    int32_t toClear = 0;
    bits::forEachSetBit(nulls->as<uint64_t>(), 0, size, [&](int32_t row) {
      // Set the values between the last non-null and this to type default.
      for (; toClear < row; ++toClear) {
        rawValues[toClear] = Timestamp();
      }
      rawValues[row] = readLosslessTimestamp(source);
      toClear = row + 1;
    });
  } else {
    for (int32_t row = 0; row < size; ++row) {
      rawValues[row] = readLosslessTimestamp(source);
    }
  }
}

void readDecimalValues(ByteStream* source, vector_size_t size, BufferPtr nulls,
                       vector_size_t nullCount, BufferPtr values) {
  auto rawValues = values->asMutable<int128_t>();
  if (nullCount != -1) {
    int32_t nonNullPositionCount = source->read<int32_t>();
    if (nonNullPositionCount) {
      int32_t sliceSize =
          Unsafe::instance().arraySliceSize<int64_t>(nonNullPositionCount * 2);

      auto rawByteValues = values->asMutable<uint8_t>();
      source->readBytes(rawByteValues, sliceSize);

      int position = 2 * (nonNullPositionCount - 1);
      auto rawNulls = nulls->as<uint64_t>();
      for (int i = size - 1; i >= 0 && position >= 0; i--) {
        memcpy(rawByteValues + position, rawByteValues + 2 * i, 2 * sizeof(int64_t));
        if (!bits::isBitNull(rawNulls, i)) {
          position -= 2;
        }
      }
    }
  } else {
    int32_t sliceSize = Unsafe::instance().arraySliceSize<int64_t>(size * 2);
    if (sliceSize) {
      source->readBytes(values->asMutable<uint8_t>(), sliceSize);
    }
  }
}

void readArrayVector(ByteStream* source, std::shared_ptr<const Type> type,
                     facebook::velox::memory::MemoryPool* pool, VectorPtr* result,
                     bool useLosslessTimestamp) {
  ArrayVector* arrayVector =
      (*result && result->unique()) ? (*result)->as<ArrayVector>() : nullptr;
  std::vector<TypePtr> childTypes = {type->childAt(0)};
  std::vector<VectorPtr> children(1);
  if (arrayVector) {
    children[0] = arrayVector->elements();
  }
  readColumns(source, pool, childTypes, &children, useLosslessTimestamp);

  vector_size_t size = source->read<int32_t>();
  if (arrayVector) {
    arrayVector->resize(size);
  } else {
    *result = BaseVector::create(type, size, pool);
    arrayVector = (*result)->as<ArrayVector>();
  }
  arrayVector->setElements(children[0]);

  BufferPtr offsets = arrayVector->mutableOffsets(size);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  BufferPtr sizes = arrayVector->mutableSizes(size);
  auto rawSizes = sizes->asMutable<vector_size_t>();
  int32_t base = source->read<int32_t>();
  for (int32_t i = 0; i < size; ++i) {
    int32_t offset = source->read<int32_t>();
    rawOffsets[i] = base;
    rawSizes[i] = offset - base;
    base = offset;
  }

  readNulls(source, size, arrayVector);
}

void readMapVector(ByteStream* source, std::shared_ptr<const Type> type,
                   facebook::velox::memory::MemoryPool* pool, VectorPtr* result,
                   bool useLosslessTimestamp) {
  MapVector* mapVector =
      (*result && result->unique()) ? (*result)->as<MapVector>() : nullptr;
  std::vector<TypePtr> childTypes = {type->childAt(0), type->childAt(1)};
  std::vector<VectorPtr> children(2);
  if (mapVector) {
    children[0] = mapVector->mapKeys();
    children[1] = mapVector->mapValues();
  }
  readColumns(source, pool, childTypes, &children, useLosslessTimestamp);

  int32_t hashTableSize = source->read<int32_t>();
  if (hashTableSize != -1) {
    // Skip over serialized hash table from Presto wire format.
    source->skip(hashTableSize * sizeof(int32_t));
  }

  vector_size_t size = source->read<int32_t>();
  if (mapVector) {
    mapVector->resize(size);
  } else {
    *result = BaseVector::create(type, size, pool);
    mapVector = (*result)->as<MapVector>();
  }
  mapVector->setKeysAndValues(children[0], children[1]);

  BufferPtr offsets = mapVector->mutableOffsets(size);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  BufferPtr sizes = mapVector->mutableSizes(size);
  auto rawSizes = sizes->asMutable<vector_size_t>();
  int32_t base = source->read<int32_t>();
  for (int32_t i = 0; i < size; ++i) {
    int32_t offset = source->read<int32_t>();
    rawOffsets[i] = base;
    rawSizes[i] = offset - base;
    base = offset;
  }

  readNulls(source, size, mapVector);
}

template <typename T>
void read(ByteStream* source, std::shared_ptr<const Type> type, memory::MemoryPool* pool,
          VectorPtr* result, bool useLosslessTimestamp) {
  int32_t size = source->read<int32_t>();
  if (*result && result->unique()) {
    (*result)->resize(size);
  } else {
    *result = BaseVector::create(type, size, pool);
  }

  auto flatResult = (*result)->asFlatVector<T>();
  auto nullCount = readNulls(source, size, flatResult);

  BufferPtr values = flatResult->mutableValues(size);
  if constexpr (std::is_same_v<T, Timestamp>) {
    if (useLosslessTimestamp) {
      readLosslessTimestampValues(source, size, flatResult->nulls(), nullCount, values);
      return;
    }
  }
  if (type->isLongDecimal()) {
    readDecimalValues(source, size, flatResult->nulls(), nullCount, values);
    return;
  }
  readValues<T>(source, size, flatResult->nulls(), nullCount, values);
}

BufferPtr findOrAllocateStringBuffer(int64_t size, const std::vector<BufferPtr>& buffers,
                                     facebook::velox::memory::MemoryPool* pool) {
  BufferPtr smallestBuffer;
  for (auto& buffer : buffers) {
    if (buffer->unique() && buffer->capacity() >= size && !buffer->isView()) {
      if (!smallestBuffer || buffer->capacity() < smallestBuffer->capacity()) {
        smallestBuffer = buffer;
      }
    }
  }
  if (smallestBuffer) {
    return smallestBuffer;
  }

  return AlignedBuffer::allocate<char>(size, pool);
}

template <>
void read<StringView>(ByteStream* source, std::shared_ptr<const Type> type,
                      facebook::velox::memory::MemoryPool* pool, VectorPtr* result,
                      bool useLosslessTimestamp) {
  int32_t size = source->read<int32_t>();

  if (*result && result->unique()) {
    (*result)->resize(size);
  } else {
    *result = BaseVector::create(type, size, pool);
  }

  // TODO: verify null string case!
  // refer to io.trino.spi.block.VariableWidthBlockEncoding#writeBlock
  int32_t nonNullsCount = source->read<int32_t>();

  int32_t lengthBufferSize = Unsafe::instance().arraySliceSize<int32_t>(nonNullsCount);
  int32_t* nonNullLengthBuffer =
      reinterpret_cast<int32_t*>(pool->allocate(lengthBufferSize));
  source->readBytes(reinterpret_cast<uint8_t*>(nonNullLengthBuffer), lengthBufferSize);

  // source->skip(skipNullsSize);
  VLOG(1) << "nonNullsCount is " << nonNullsCount << ", lengthBufferSize is "
          << lengthBufferSize;

  auto flatResult = (*result)->as<FlatVector<StringView>>();
  readNulls(source, size, flatResult);

  int32_t dataSize = source->read<int32_t>();
  const auto& stringBuffers = flatResult->stringBuffers();
  BufferPtr strings = findOrAllocateStringBuffer(dataSize, stringBuffers, pool);
  flatResult->setStringBuffers({strings});
  auto rawStrings = strings->asMutable<uint8_t>();

  BufferPtr values = flatResult->mutableValues(size);
  auto rawValues = values->asMutable<StringView>();
  source->readBytes(rawStrings, dataSize);
  int32_t previousOffset = 0;
  auto rawChars = reinterpret_cast<char*>(rawStrings);
  int32_t nonNullIndex = 0;
  for (int32_t i = 0; i < size; ++i) {
    if (flatResult->isNullAt(i)) {
      rawValues[i] = StringView();
    } else {
      int32_t length = nonNullLengthBuffer[nonNullIndex];
      rawValues[i] = StringView(rawChars + previousOffset, length);
      previousOffset += length;
      ++nonNullIndex;
    }
  }
  pool->free(reinterpret_cast<void*>(nonNullLengthBuffer), lengthBufferSize);
}

void readTimestampWithTimeZone(ByteStream* source,
                               facebook::velox::memory::MemoryPool* pool,
                               VectorPtr* result) {
  VectorPtr timestamps;
  read<int64_t>(source, BIGINT(), pool, &timestamps, false);

  auto rawTimestamps = timestamps->asFlatVector<int64_t>()->mutableRawValues();

  auto size = timestamps->size();

  auto timezones = BaseVector::create(SMALLINT(), size, pool);
  auto rawTimezones = timezones->asFlatVector<int16_t>()->mutableRawValues();

  auto rawNulls = timestamps->rawNulls();
  for (auto i = 0; i < size; ++i) {
    if (!rawNulls || !bits::isBitNull(rawNulls, i)) {
      unpackTimestampWithTimeZone(rawTimestamps[i], rawTimestamps[i], rawTimezones[i]);
    }
  }

  *result =
      std::make_shared<RowVector>(pool, TIMESTAMP_WITH_TIME_ZONE(), timestamps->nulls(),
                                  size, std::vector<VectorPtr>{timestamps, timezones});
}

void readRowVector(ByteStream* source, std::shared_ptr<const Type> type,
                   facebook::velox::memory::MemoryPool* pool, VectorPtr* result,
                   bool useLosslessTimestamp) {
  if (isTimestampWithTimeZoneType(type)) {
    readTimestampWithTimeZone(source, pool, result);
    return;
  }

  int32_t numChildren = source->read<int32_t>();
  auto size = source->read<int32_t>();

  RowVector* reused =
      (*result && result->unique()) ? (*result)->template as<RowVector>() : nullptr;
  if (reused && (reused->childrenSize() != numChildren || reused->type() != type)) {
    reused = nullptr;
  }

  std::vector<VectorPtr> tempChildren;
  std::vector<VectorPtr>* children;
  if (reused) {
    children = &reused->children();
  } else {
    tempChildren.resize(numChildren);
    children = &tempChildren;
  }

  auto childTypes = type->as<TypeKind::ROW>().children();
  readColumns(source, pool, childTypes, children, useLosslessTimestamp);

  if (reused) {
    reused->resize(size);
  } else {
    *result = BaseVector::create(type, size, pool);
    reused = (*result)->as<RowVector>();
    reused->children() = tempChildren;
  }

  vector_size_t* rawOffsets = nullptr;
  BufferPtr offsets(nullptr);
  bool needOffsets = false;
  for (int32_t i = 0; i <= size; ++i) {
    int32_t childOffset = 0;
    // int32_t childOffset = source->read<int32_t>();
    // if (childOffset != i) {
    if (false) {
      needOffsets = true;
      if (!rawOffsets) {
        BaseVector::resizeIndices(size, pool, &offsets,
                                  const_cast<const vector_size_t**>(&rawOffsets), 0);
        for (int32_t child = 0; child < i; ++child) {
          rawOffsets[child] = child;
        }
      }
      rawOffsets[i] = childOffset;
    }
  }

  auto nullCount = readNulls(source, size, reused);

  // refer to io.trino.spi.block.RowBlockEncoding#readBlock
  // todo: handle fieldBlockOffsets
  // It should be 'size + 1' in Trino, but 'size' indeed works.
  if (nullCount != -1) {
    int32_t fieldBlockOffsetsSkipSize =
        Unsafe::instance().arraySliceSize<int32_t>(size + 1);
    source->skip(fieldBlockOffsetsSkipSize);
    VLOG(1) << "fieldBlockOffsetsSkipSize is " << fieldBlockOffsetsSkipSize;
  }

  for (auto& range : source->ranges()) {
    VLOG(1) << "range size is " << range.size << ", range position is " << range.position;
  }

  // if offsets is needed, reconstruct aligned layout.
  // TODO: make it efficient
  if (needOffsets) {
    tempChildren.resize(numChildren);
    auto sourceRow = dynamic_cast<RowVector*>((*result).get());
    for (int32_t child = 0; child < numChildren; ++child) {
      tempChildren[child] = BaseVector::create(childTypes[child], size, pool);
      auto src = sourceRow->childAt(child);
      for (int32_t i = 0; i < size; ++i) {
        if (!sourceRow->isNullAt(i)) {
          tempChildren[child]->copy(src.get(), i, rawOffsets[i], 1);
        }
      }
    }
    *result =
        std::make_shared<RowVector>(pool, type, (*result)->nulls(), size,
                                    std::move(tempChildren), (*result)->getNullCount());
  }
}

std::string readLengthPrefixedString(ByteStream* source) {
  int32_t size = source->read<int32_t>();
  std::string value;
  value.resize(size);
  source->readBytes(&value[0], size);
  return value;
}

void checkTypeEncoding(std::string encoding, TypePtr type) {
  auto kindEncoding = typeToEncodingName(type);
  VELOX_CHECK(encoding == kindEncoding, "Encoding to Type mismatch {} expected {} got {}",
              type->kindName(), kindEncoding, encoding);
}

void readConstantVector(ByteStream* source, const TypePtr& type, memory::MemoryPool* pool,
                        VectorPtr* result, bool useLosslessTimestamp) {
  auto size = source->read<int32_t>();
  std::vector<TypePtr> childTypes = {type};
  std::vector<VectorPtr> children(1);
  readColumns(source, pool, childTypes, &children, useLosslessTimestamp);
  VELOX_CHECK_EQ(1, children[0]->size());
  *result = BaseVector::wrapInConstant(size, 0, children[0]);
}

void readColumns(ByteStream* source, memory::MemoryPool* pool,
                 const std::vector<TypePtr>& types, std::vector<VectorPtr>* result,
                 bool useLosslessTimestamp) {
  static std::unordered_map<
      TypeKind, std::function<void(ByteStream * source, std::shared_ptr<const Type> type,
                                   memory::MemoryPool * pool, VectorPtr * result,
                                   bool useLosslessTimestamp)>>
      readers = {
          {TypeKind::BOOLEAN, &read<bool>},
          {TypeKind::TINYINT, &read<int8_t>},
          {TypeKind::SMALLINT, &read<int16_t>},
          {TypeKind::INTEGER, &read<int32_t>},
          {TypeKind::BIGINT, &read<int64_t>},
          {TypeKind::HUGEINT, &read<int128_t>},
          {TypeKind::REAL, &read<float>},
          {TypeKind::DOUBLE, &read<double>},
          {TypeKind::TIMESTAMP, &read<Timestamp>},
          {TypeKind::VARCHAR, &read<StringView>},
          {TypeKind::VARBINARY, &read<StringView>},
          {TypeKind::ARRAY, &readArrayVector},
          {TypeKind::MAP, &readMapVector},
          {TypeKind::ROW, &readRowVector}  //,
                                           //{TypeKind::UNKNOWN, &read<UnknownValue>}
      };

  for (int32_t i = 0; i < types.size(); ++i) {
    auto encoding = readLengthPrefixedString(source);
    VLOG(1) << "Reading column type " << encoding;
    if (encoding == kRLE) {
      readConstantVector(source, types[i], pool, &(*result)[i], useLosslessTimestamp);
    } else {
      checkTypeEncoding(encoding, types[i]);
      auto it = readers.find(types[i]->kind());
      VELOX_CHECK(it != readers.end(), "Column reader for type {} is missing",
                  types[i]->kindName());

      it->second(source, types[i], pool, &(*result)[i], useLosslessTimestamp);
    }
  }
}

void TrinoVectorSerde::deserialize(facebook::velox::ByteStream* source,
                                   facebook::velox::memory::MemoryPool* pool,
                                   facebook::velox::RowTypePtr type,
                                   facebook::velox::RowVectorPtr* result,
                                   const Options* options) {
  bool useLosslessTimestamp = false;
  // options != nullptr
  //     ? static_cast<const PrestoOptions*>(options)->useLosslessTimestamp
  //     : false;
  auto numRows = source->read<int32_t>();
  VLOG(1) << "numRows is " << numRows;
  if (!(*result) || !result->unique() || (*result)->type() != type) {
    *result =
        std::dynamic_pointer_cast<RowVector>(BaseVector::create(type, numRows, pool));
  } else if (numRows != 0) {
    (*result)->resize(numRows);
  }

  auto pageCodecMarker = source->read<int8_t>();
  VLOG(1) << "pageCodecMarker is " << pageCodecMarker;
  if (numRows == 0) {
    auto nullRowNumber = source->read<int32_t>();
    VLOG(1) << "Read a blank value if null row number " << nullRowNumber;
  }
  auto uncompressedSize = source->read<int32_t>();
  VLOG(1) << "uncompressedSize is " << uncompressedSize;
  // skip size in bytes (compressed size)
  auto compressedSize = source->read<int32_t>();
  VLOG(1) << "compressedSize is " << compressedSize;
  // source->skip(4);

  // skip number of columns
  auto numColumns = source->read<int32_t>();
  VLOG(1) << "numColumns is " << numColumns;
  // source->skip(4);

  auto children = &(*result)->children();
  auto childTypes = type->as<TypeKind::ROW>().children();
  VELOX_CHECK_EQ(numColumns, childTypes.size(),
                 "The size of childTypes must be equal to numColumns.");
  readColumns(source, pool, childTypes, children, useLosslessTimestamp);
}

void expandRepeatedRanges(const BaseVector* vector, const vector_size_t* rawOffsets,
                          const vector_size_t* rawSizes,
                          const folly::Range<const IndexRange*>& ranges,
                          vector_size_t** sizes, std::vector<IndexRange>* childRanges,
                          std::vector<vector_size_t*>* childSizes) {
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t begin = ranges[i].begin;
    int32_t end = begin + ranges[i].size;
    *sizes[i] += sizeof(int32_t);
    for (int32_t offset = begin; offset < end; ++offset) {
      if (!vector->isNullAt(offset)) {
        childRanges->push_back(IndexRange{rawOffsets[offset], rawSizes[offset]});
        childSizes->push_back(sizes[i]);
      }
    }
  }
}

template <TypeKind Kind>
void estimateFlatSerializedSize(const BaseVector* vector,
                                const folly::Range<const IndexRange*>& ranges,
                                vector_size_t** sizes) {
  auto valueSize = vector->type()->cppSizeInBytes();
  if (vector->mayHaveNulls()) {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      auto end = ranges[i].begin + ranges[i].size;
      int32_t numNulls = 0;
      int32_t bytes = 0;
      auto rawNulls = vector->rawNulls();
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (bits::isBitNull(rawNulls, offset)) {
          ++numNulls;
        } else {
          bytes += valueSize;
        }
      }
      *(sizes[i]) += bytes + bits::nbytes(numNulls);
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      *(sizes[i]) += ranges[i].size * valueSize;
    }
  }
}

void estimateFlatSerializedSizeVarcharOrVarbinary(
    const BaseVector* vector, const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  auto strings = static_cast<const FlatVector<StringView>*>(vector);
  auto rawNulls = strings->rawNulls();
  auto rawValues = strings->rawValues();
  for (int32_t i = 0; i < ranges.size(); ++i) {
    auto end = ranges[i].begin + ranges[i].size;
    int32_t numNulls = 0;
    int32_t bytes = 0;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (rawNulls && bits::isBitNull(rawNulls, offset)) {
        ++numNulls;
      } else {
        bytes += sizeof(int32_t) + rawValues[offset].size();
      }
    }
    *(sizes[i]) += bytes + bits::nbytes(numNulls) + 4 * numNulls;
  }
}

template <>
void estimateFlatSerializedSize<TypeKind::VARCHAR>(
    const BaseVector* vector, const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  estimateFlatSerializedSizeVarcharOrVarbinary(vector, ranges, sizes);
}

template <>
void estimateFlatSerializedSize<TypeKind::VARBINARY>(
    const BaseVector* vector, const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  estimateFlatSerializedSizeVarcharOrVarbinary(vector, ranges, sizes);
}

void estimateBiasedSerializedSize(const BaseVector* vector,
                                  const folly::Range<const IndexRange*>& ranges,
                                  vector_size_t** sizes) {
  auto valueSize = vector->type()->cppSizeInBytes();
  if (vector->mayHaveNulls()) {
    auto rawNulls = vector->rawNulls();
    for (int32_t i = 0; i < ranges.size(); ++i) {
      auto end = ranges[i].begin + ranges[i].size;
      int32_t numNulls = 0;
      int32_t bytes = 0;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (bits::isBitNull(rawNulls, offset)) {
          ++numNulls;
        } else {
          bytes += valueSize;
        }
      }
      *(sizes[i]) += bytes + bits::nbytes(numNulls);
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      *(sizes[i]) += ranges[i].size * valueSize;
    }
  }
}

void estimateSerializedSizeInt(const BaseVector* vector,
                               const folly::Range<const IndexRange*>& ranges,
                               vector_size_t** sizes);

void estimateWrapperSerializedSize(const folly::Range<const IndexRange*>& ranges,
                                   vector_size_t** sizes, const BaseVector* wrapper) {
  std::vector<IndexRange> newRanges;
  std::vector<vector_size_t*> newSizes;
  const BaseVector* wrapped = wrapper->wrappedVector();
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t numNulls = 0;
    auto end = ranges[i].begin + ranges[i].size;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (!wrapper->isNullAt(offset)) {
        newRanges.push_back(IndexRange{wrapper->wrappedIndex(offset), 1});
        newSizes.push_back(sizes[i]);
      } else {
        ++numNulls;
      }
    }
    *sizes[i] += bits::nbytes(numNulls);
  }
  estimateSerializedSizeInt(wrapped, newRanges, newSizes.data());
}

template <TypeKind Kind>
void estimateConstantSerializedSize(const BaseVector* vector,
                                    const folly::Range<const IndexRange*>& ranges,
                                    vector_size_t** sizes) {
  VELOX_CHECK(vector->encoding() == VectorEncoding::Simple::CONSTANT);
  using T = typename KindToFlatVector<Kind>::WrapperType;
  auto constantVector = vector->as<ConstantVector<T>>();
  if (constantVector->valueVector()) {
    estimateWrapperSerializedSize(ranges, sizes, vector);
    return;
  }
  int32_t elementSize = sizeof(T);
  if (constantVector->isNullAt(0)) {
    elementSize = 1;
  } else if (std::is_same_v<T, StringView>) {
    auto value = constantVector->valueAt(0);
    auto string = reinterpret_cast<const StringView*>(&value);
    elementSize = string->size();
  }
  for (int32_t i = 0; i < ranges.size(); ++i) {
    *sizes[i] += elementSize * ranges[i].size;
  }
}

void estimateSerializedSizeInt(const BaseVector* vector,
                               const folly::Range<const IndexRange*>& ranges,
                               vector_size_t** sizes) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(estimateFlatSerializedSize,
                                             vector->typeKind(), vector, ranges, sizes);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(estimateConstantSerializedSize, vector->typeKind(),
                                      vector, ranges, sizes);
      break;
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      estimateWrapperSerializedSize(ranges, sizes, vector);
      break;
    case VectorEncoding::Simple::BIASED:
      estimateBiasedSerializedSize(vector, ranges, sizes);
      break;
    case VectorEncoding::Simple::ROW: {
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      for (int32_t i = 0; i < ranges.size(); ++i) {
        auto begin = ranges[i].begin;
        auto end = begin + ranges[i].size;
        for (auto offset = begin; offset < end; ++offset) {
          *sizes[i] += sizeof(int32_t);
          if (!vector->isNullAt(offset)) {
            childRanges.push_back(IndexRange{offset, 1});
            childSizes.push_back(sizes[i]);
          }
        }
      }
      auto rowVector = vector->as<RowVector>();
      auto children = rowVector->children();
      for (auto& child : children) {
        if (child) {
          estimateSerializedSizeInt(child.get(),
                                    folly::Range(childRanges.data(), childRanges.size()),
                                    childSizes.data());
        }
      }
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto mapVector = vector->as<MapVector>();
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      expandRepeatedRanges(mapVector, mapVector->rawOffsets(), mapVector->rawSizes(),
                           ranges, sizes, &childRanges, &childSizes);
      estimateSerializedSizeInt(mapVector->mapKeys().get(), childRanges,
                                childSizes.data());
      estimateSerializedSizeInt(mapVector->mapValues().get(), childRanges,
                                childSizes.data());
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto arrayVector = vector->as<ArrayVector>();
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      expandRepeatedRanges(arrayVector, arrayVector->rawOffsets(),
                           arrayVector->rawSizes(), ranges, sizes, &childRanges,
                           &childSizes);
      estimateSerializedSizeInt(arrayVector->elements().get(), childRanges,
                                childSizes.data());
      break;
    }
    case VectorEncoding::Simple::LAZY:
      estimateSerializedSizeInt(vector->loadedVector(), ranges, sizes);
      break;
    default:
      VELOX_CHECK(false, "Unsupported vector encoding {}", vector->encoding());
  }
}

void TrinoVectorSerde::estimateSerializedSize(
    VectorPtr vector, const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  estimateSerializedSizeInt(vector->loadedVector(), ranges, sizes);
}

// static
void TrinoVectorSerde::registerVectorSerde() {
  facebook::velox::registerVectorSerde(std::make_unique<TrinoVectorSerde>());
}

}  // namespace io::trino::bridge
