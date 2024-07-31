/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef UTILS_ARROW_UTILS_H_
#define UTILS_ARROW_UTILS_H_

#include <arrow/api.h>
#include <arrow/util/value_parsing.h>
#include <memory>
#include "flex/utils/property/types.h"
#include "glog/logging.h"

namespace gs {

// arrow related;

class LDBCTimeStampParser : public arrow::TimestampParser {
 public:
  LDBCTimeStampParser() = default;

  ~LDBCTimeStampParser() override {}

  bool operator()(const char* s, size_t length, arrow::TimeUnit::type out_unit,
                  int64_t* out,
                  bool* out_zone_offset_present = NULLPTR) const override {
    using seconds_type = std::chrono::duration<arrow::TimestampType::c_type>;

    // We allow the following zone offset formats:
    // - (none)
    // - Z
    // - [+-]HH(:?MM)?
    //
    // We allow the following formats for all units:
    // - "YYYY-MM-DD"
    // - "YYYY-MM-DD[ T]hhZ?"
    // - "YYYY-MM-DD[ T]hh:mmZ?"
    // - "YYYY-MM-DD[ T]hh:mm:ssZ?"
    //
    // We allow the following formats for unit == MILLI, MICRO, or NANO:
    // - "YYYY-MM-DD[ T]hh:mm:ss.s{1,3}Z?"
    //
    // We allow the following formats for unit == MICRO, or NANO:
    // - "YYYY-MM-DD[ T]hh:mm:ss.s{4,6}Z?"
    //
    // We allow the following formats for unit == NANO:
    // - "YYYY-MM-DD[ T]hh:mm:ss.s{7,9}Z?"
    //
    // UTC is always assumed, and the DataType's timezone is ignored.
    //

    if (ARROW_PREDICT_FALSE(length < 10))
      return false;

    seconds_type seconds_since_epoch;
#if defined(ARROW_VERSION) && ARROW_VERSION < 15000000
    if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseYYYY_MM_DD(
            s, &seconds_since_epoch))) {
#else
    if (ARROW_PREDICT_FALSE(
            !arrow::internal::ParseYYYY_MM_DD(s, &seconds_since_epoch))) {
#endif
      return false;
    }

    if (length == 10) {
      *out =
          arrow::util::CastSecondsToUnit(out_unit, seconds_since_epoch.count());
      return true;
    }

    if (ARROW_PREDICT_FALSE(s[10] != ' ') &&
        ARROW_PREDICT_FALSE(s[10] != 'T')) {
      return false;
    }

    // In the implementation of arrow ISO8601 timestamp parser, the zone offset
    // is set to true if the input string contains a zone offset. However, we
    // parse the zone offset here but don't set the boolean flag.
    // https://github.com/apache/arrow/blob/3e7ae5340a123c1040f98f1c36687b81362fab52/cpp/src/arrow/csv/converter.cc#L373
    // The reason is that, if we want the zone offset to be set, we need to
    // to declare the zone offset in the schema and construct TimeStampType with
    // that offset. However, we just want to parse the timestamp string and
    // convert it to a timestamp value, we have no assumption of the local time
    // zone, and we don't require the zone offset to be set in the schema.
    // Same for following commented code.
    //-------------------------------------------------------------------------
    // if (out_zone_offset_present) {
    //   *out_zone_offset_present = false;
    // }
    //-------------------------------------------------------------------------

    seconds_type zone_offset(0);
    if (s[length - 1] == 'Z') {
      --length;
      // if (out_zone_offset_present)
      //   *out_zone_offset_present = true;
    } else if (s[length - 3] == '+' || s[length - 3] == '-') {
      // [+-]HH
      length -= 3;
      if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseHH(
              s + length + 1, &zone_offset))) {
        return false;
      }
      if (s[length] == '+')
        zone_offset *= -1;
      // if (out_zone_offset_present)
      //   *out_zone_offset_present = true;
    } else if (s[length - 5] == '+' || s[length - 5] == '-') {
      // [+-]HHMM
      length -= 5;
      if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseHHMM(
              s + length + 1, &zone_offset))) {
        return false;
      }
      if (s[length] == '+')
        zone_offset *= -1;
      // if (out_zone_offset_present)
      //   *out_zone_offset_present = true;
    } else if ((s[length - 6] == '+' || s[length - 6] == '-') &&
               (s[length - 3] == ':')) {
      // [+-]HH:MM
      length -= 6;
      if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseHH_MM(
              s + length + 1, &zone_offset))) {
        return false;
      }
      if (s[length] == '+')
        zone_offset *= -1;
      // if (out_zone_offset_present)
      //   *out_zone_offset_present = true;
    }

    seconds_type seconds_since_midnight;
    switch (length) {
    case 13:  // YYYY-MM-DD[ T]hh
      if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseHH(
              s + 11, &seconds_since_midnight))) {
        return false;
      }
      break;
    case 16:  // YYYY-MM-DD[ T]hh:mm
      if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseHH_MM(
              s + 11, &seconds_since_midnight))) {
        return false;
      }
      break;
    case 19:  // YYYY-MM-DD[ T]hh:mm:ss
    case 21:  // YYYY-MM-DD[ T]hh:mm:ss.s
    case 22:  // YYYY-MM-DD[ T]hh:mm:ss.ss
    case 23:  // YYYY-MM-DD[ T]hh:mm:ss.sss
    case 24:  // YYYY-MM-DD[ T]hh:mm:ss.ssss
    case 25:  // YYYY-MM-DD[ T]hh:mm:ss.sssss
    case 26:  // YYYY-MM-DD[ T]hh:mm:ss.ssssss
    case 27:  // YYYY-MM-DD[ T]hh:mm:ss.sssssss
    case 28:  // YYYY-MM-DD[ T]hh:mm:ss.ssssssss
    case 29:  // YYYY-MM-DD[ T]hh:mm:ss.sssssssss
      if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseHH_MM_SS(
              s + 11, &seconds_since_midnight))) {
        return false;
      }
      break;
    default:
      LOG(ERROR) << "unsupported length: " << length;
      return false;
    }

    seconds_since_epoch += seconds_since_midnight;
    seconds_since_epoch += zone_offset;

    if (length <= 19) {
      *out =
          arrow::util::CastSecondsToUnit(out_unit, seconds_since_epoch.count());
      return true;
    }

    if (ARROW_PREDICT_FALSE(s[19] != '.')) {
      return false;
    }

    uint32_t subseconds = 0;
    if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseSubSeconds(
            s + 20, length - 20, out_unit, &subseconds))) {
      return false;
    }

    *out =
        arrow::util::CastSecondsToUnit(out_unit, seconds_since_epoch.count()) +
        subseconds;
    return true;
  }

  const char* kind() const override { return "LDBC timestamp parser"; }

  const char* format() const override { return "EmptyFormat"; }
};

class LDBCLongDateParser : public arrow::TimestampParser {
 public:
  using seconds_type = std::chrono::duration<arrow::TimestampType::c_type>;
  LDBCLongDateParser() = default;

  ~LDBCLongDateParser() override {}

  bool operator()(const char* s, size_t length, arrow::TimeUnit::type out_unit,
                  int64_t* out,
                  bool* out_zone_offset_present = NULLPTR) const override {
    uint64_t seconds;
    // convert (s, s + length - 4) to seconds_since_epoch
    if (ARROW_PREDICT_FALSE(
            !arrow::internal::ParseUnsigned(s, length - 3, &seconds))) {
      return false;
    }

    uint32_t subseconds = 0;
    if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseSubSeconds(
            s + length - 3, 3, out_unit, &subseconds))) {
      return false;
    }

    *out = arrow::util::CastSecondsToUnit(out_unit, seconds) + subseconds;
    return true;
  }

  const char* kind() const override { return "LDBC timestamp parser"; }

  const char* format() const override { return "LongDateFormat"; }
};

// convert c++ type to arrow type. support other types likes emptyType, Date
template <typename T>
struct TypeConverter;

template <>
struct TypeConverter<bool> {
  static PropertyType property_type() { return PropertyType::kBool; }
  using ArrowType = arrow::BooleanType;
  using ArrowArrayType = arrow::BooleanArray;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::boolean();
  }
};

template <>
struct TypeConverter<int32_t> {
  static PropertyType property_type() { return PropertyType::kInt32; }
  using ArrowType = arrow::Int32Type;
  using ArrowArrayType = arrow::Int32Array;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::int32();
  }
};

template <>
struct TypeConverter<uint32_t> {
  static PropertyType property_type() { return PropertyType::kUInt32; }
  using ArrowType = arrow::UInt32Type;
  using ArrowArrayType = arrow::UInt32Array;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::uint32();
  }
};

template <>
struct TypeConverter<int64_t> {
  static PropertyType property_type() { return PropertyType::kInt64; }
  using ArrowType = arrow::Int64Type;
  using ArrowArrayType = arrow::Int64Array;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::int64();
  }
};

template <>
struct TypeConverter<uint64_t> {
  static PropertyType property_type() { return PropertyType::kUInt64; }
  using ArrowType = arrow::UInt64Type;
  using ArrowArrayType = arrow::UInt64Array;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::uint64();
  }
};

template <>
struct TypeConverter<double> {
  static PropertyType property_type() { return PropertyType::kDouble; }
  using ArrowType = arrow::DoubleType;
  using ArrowArrayType = arrow::DoubleArray;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::float64();
  }
};

template <>
struct TypeConverter<float> {
  static PropertyType property_type() { return PropertyType::kFloat; }
  using ArrowType = arrow::FloatType;
  using ArrowArrayType = arrow::FloatArray;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::float32();
  }
};
template <>
struct TypeConverter<std::string> {
  static PropertyType property_type() { return PropertyType::kStringView; }
  using ArrowType = arrow::LargeStringType;
  using ArrowArrayType = arrow::LargeStringArray;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::large_utf8();
  }
};

template <>
struct TypeConverter<std::string_view> {
  static PropertyType property_type() { return PropertyType::kStringView; }
  using ArrowType = arrow::LargeStringType;
  using ArrowArrayType = arrow::LargeStringArray;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::large_utf8();
  }
};

template <>
struct TypeConverter<Date> {
  static PropertyType property_type() { return PropertyType::kDate; }
  using ArrowType = arrow::TimestampType;
  using ArrowArrayType = arrow::TimestampArray;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  }
};

template <>
struct TypeConverter<Day> {
  static PropertyType property_type() { return PropertyType::kDay; }
  using ArrowType = arrow::TimestampType;
  using ArrowArrayType = arrow::TimestampArray;
  static std::shared_ptr<arrow::DataType> ArrowTypeValue() {
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  }
};

std::shared_ptr<arrow::DataType> PropertyTypeToArrowType(PropertyType type);
}  // namespace gs

#endif  // UTILS_ARROW_UTILS_H_
