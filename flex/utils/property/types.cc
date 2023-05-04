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

#include "flex/utils/property/types.h"

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

inline void ParseInt32(const std::string_view& str, int& val) {
  sscanf(str.data(), "%d", &val);
}

inline void ParseInt64(const std::string_view& str, int64_t& val) {
#ifdef __APPLE__
  sscanf(str.data(), "%lld", &val);
#else
  sscanf(str.data(), "%" SCNd64, &val);
#endif
}

inline void ParseDate(const std::string_view& str, Date& date) {
  date.reset(str.data());
}

inline void ParseString(const std::string_view& str, std::string_view& val) {
  val = str;
}

void ParseRecord(const char* line, std::vector<Property>& rec) {
  const char* cur = line;
  for (auto& item : rec) {
    const char* ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view sv(cur, ptr - cur);
    if (item.type() == PropertyType::kInt32) {
      int val;
      ParseInt32(sv, val);
      item.set_value<int>(val);
    } else if (item.type() == PropertyType::kInt64) {
      int64_t val;
      ParseInt64(sv, val);
      item.set_value<int64_t>(val);
    } else if (item.type() == PropertyType::kDate) {
      Date val;
      ParseDate(sv, val);
      item.set_value<Date>(val);
    } else if (item.type() == PropertyType::kStringView) {
      std::string_view val;
      ParseString(sv, val);
      item.set_value<std::string_view>(val);
    } else if (item.type() == PropertyType::kString) {
      std::string val(sv);
      item.set_value<std::string>(val);
    } else {
      LOG(FATAL) << "Unexpected property type: "
                 << static_cast<int>(item.type()) << ".";
    }
    cur = ptr + 1;
  }
}

void ParseRecord(const char* line, int64_t& id, std::vector<Property>& rec) {
  const char* cur = line;
  {
    const char* ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view sv(cur, ptr - cur);
    ParseInt64(sv, id);
    cur = ptr + 1;
  }
  ParseRecord(cur, rec);
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%d", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%d", &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int64_t& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld|%lld", &src, &dst, &prop);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "|%" SCNd64, &src, &dst, &prop);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, Date& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld", &src, &dst);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "", &src, &dst);
#endif
  const char* ptr = strrchr(line, '|') + 1;
  prop.reset(ptr);
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  grape::EmptyType& prop) {
#ifdef __APPLE__
  sscanf(line, "%lld|%lld", &src, &dst);
#else
  sscanf(line, "%" SCNd64 "|%" SCNd64 "", &src, &dst);
#endif
}

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  std::vector<Property>& rec) {
  const char* cur = line;
  {
    const char* ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view src_sv(cur, ptr - cur);
    ParseInt64(src_sv, src);
    cur = ptr + 1;

    ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view dst_sv(cur, ptr - cur);
    ParseInt64(dst_sv, dst);
    cur = ptr + 1;
  }
  ParseRecord(cur, rec);
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const Property& value) {
  PropertyType type = value.type();
  in_archive << type;
  switch (type) {
  case PropertyType::kEmpty:
    break;
  case PropertyType::kInt8:
    in_archive << value.get_value<int8_t>();
    break;
  case PropertyType::kUInt8:
    in_archive << value.get_value<uint8_t>();
    break;
  case PropertyType::kInt16:
    in_archive << value.get_value<int16_t>();
    break;
  case PropertyType::kUInt16:
    in_archive << value.get_value<uint16_t>();
    break;
  case PropertyType::kInt32:
    in_archive << value.get_value<int32_t>();
    break;
  case PropertyType::kUInt32:
    in_archive << value.get_value<uint32_t>();
    break;
  case PropertyType::kInt64:
    in_archive << value.get_value<int64_t>();
    break;
  case PropertyType::kUInt64:
    in_archive << value.get_value<uint64_t>();
    break;
  case PropertyType::kDate:
    in_archive << value.get_value<Date>().milli_second;
    break;
  case PropertyType::kFloat:
    in_archive << value.get_value<float>();
    break;
  case PropertyType::kDouble:
    in_archive << value.get_value<double>();
    break;
  case PropertyType::kString:
    in_archive << value.get_value<std::string>();
    break;
  case PropertyType::kStringView:
    in_archive << value.get_value<std::string_view>();
    break;
  case PropertyType::kList:
    in_archive << value.get_value<std::vector<Property>>();
    break;
  }

  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Property& value) {
  PropertyType type;
  out_archive >> type;
  switch (type) {
  case PropertyType::kEmpty:
    value.set_value<grape::EmptyType>(grape::EmptyType());
    break;
  case PropertyType::kInt8:
    int8_t i8;
    out_archive >> i8;
    value.set_value<int8_t>(i8);
    break;
  case PropertyType::kUInt8:
    uint8_t ui8;
    out_archive >> ui8;
    value.set_value<uint8_t>(ui8);
    break;
  case PropertyType::kInt16:
    int16_t i16;
    out_archive >> i16;
    value.set_value<int16_t>(i16);
    break;
  case PropertyType::kUInt16:
    uint16_t ui16;
    out_archive >> ui16;
    value.set_value<uint16_t>(ui16);
    break;
  case PropertyType::kInt32:
    int32_t i32;
    out_archive >> i32;
    value.set_value<int32_t>(i32);
    break;
  case PropertyType::kUInt32:
    uint32_t ui32;
    out_archive >> ui32;
    value.set_value<uint32_t>(ui32);
    break;
  case PropertyType::kInt64:
    int64_t i64;
    out_archive >> i64;
    value.set_value<int64_t>(i64);
    break;
  case PropertyType::kUInt64:
    uint64_t ui64;
    out_archive >> ui64;
    value.set_value<uint64_t>(ui64);
    break;
  case PropertyType::kDate:
    Date date;
    out_archive >> date.milli_second;
    value.set_value<Date>(date);
    break;
  case PropertyType::kFloat:
    float f;
    out_archive >> f;
    value.set_value<float>(f);
    break;
  case PropertyType::kDouble:
    double d;
    out_archive >> d;
    value.set_value<double>(d);
    break;
  case PropertyType::kString: {
    std::string s;
    out_archive >> s;
    value.set_value<std::string>(s);
  } break;
  case PropertyType::kStringView: {
    std::string_view sv;
    out_archive >> sv;
    value.set_value<std::string_view>(sv);
  } break;
  case PropertyType::kList: {
    std::vector<Property> list;
    out_archive >> list;
    value.set_value<std::vector<Property>>(list);
  } break;
  }

  return out_archive;
}

// date format:
// YYYY-MM-DD'T'hh:mm:ss.SSSZZZZ
// 2010-04-25T05:45:11.772+0000

inline static uint32_t char_to_digit(char c) { return (c - '0'); }

inline static uint32_t str_4_to_number(const char* str) {
  return char_to_digit(str[0]) * 1000u + char_to_digit(str[1]) * 100u +
         char_to_digit(str[2]) * 10u + char_to_digit(str[3]);
}

inline static uint32_t str_3_to_number(const char* str) {
  return char_to_digit(str[0]) * 100u + char_to_digit(str[1]) * 10u +
         char_to_digit(str[2]);
}

inline static uint32_t str_2_to_number(const char* str) {
  return char_to_digit(str[0]) * 10u + char_to_digit(str[1]);
}

Date::Date(const Date& x) : milli_second(x.milli_second) {}
Date::Date(int64_t x) : milli_second(x) {}
Date::Date(const char* str) { reset(str); }

void Date::reset(const char* str) {
  if (str[4] == '-') {
    struct tm v;
    memset(&v, 0, sizeof(v));
    v.tm_year = str_4_to_number(str) - 1900;
    v.tm_mon = str_2_to_number(&str[5]) - 1;
    v.tm_mday = str_2_to_number(&str[8]);
    if (str[10] == '|') {
      milli_second = mktime(&v);
      milli_second *= 1000l;
      milli_second += 8 * 60 * 60 * 1000l;
      return;
    }
    v.tm_hour = str_2_to_number(&str[11]);
    v.tm_min = str_2_to_number(&str[14]);
    v.tm_sec = str_2_to_number(&str[17]);

    milli_second = (mktime(&v));

    milli_second *= 1000l;
    milli_second += str_3_to_number(&str[20]);
    bool zone_flag = (str[23] == '+') ? 1u : 0u;
    uint32_t zone_hour = str_2_to_number(&str[24]);
    uint32_t zone_minute = str_2_to_number(&str[26]);
    milli_second += 8 * 60 * 60 * 1000l;
    if (zone_flag) {
      milli_second += (zone_hour * 60 * 60l + zone_minute * 60l) * 1000l;
    } else {
      milli_second -= (zone_hour * 60 * 60l + zone_minute * 60l) * 1000l;
    }
  } else {
#ifdef __APPLE__
    sscanf(str, "%lld", &milli_second);
#else
    sscanf(str, "%" SCNd64, &milli_second);
#endif
  }
}

std::string Date::to_string() const { return std::to_string(milli_second); }

grape::InArchive& operator<<(grape::InArchive& arc, const Date& v) {
  arc << v.milli_second;
  return arc;
}
grape::OutArchive& operator>>(grape::OutArchive& arc, Date& v) {
  arc >> v.milli_second;
  return arc;
}

}  // namespace gs
