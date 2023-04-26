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

void ParseRecord(const char* line, std::vector<Any>& rec) {
  const char* cur = line;
  for (auto& item : rec) {
    const char* ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view sv(cur, ptr - cur);
    if (item.type == PropertyType::kInt32) {
      ParseInt32(sv, item.value.i);
    } else if (item.type == PropertyType::kInt64) {
      ParseInt64(sv, item.value.l);
    } else if (item.type == PropertyType::kDate) {
      ParseDate(sv, item.value.d);
    } else if (item.type == PropertyType::kString) {
      ParseString(sv, item.value.s);
    }
    cur = ptr + 1;
  }
}

void ParseRecord(const char* line, int64_t& id, std::vector<Any>& rec) {
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

grape::InArchive& operator<<(grape::InArchive& in_archive, const Any& value) {
  switch (value.type) {
  case PropertyType::kInt32:
    in_archive << value.type << value.value.i;
    break;
  case PropertyType::kInt64:
    in_archive << value.type << value.value.l;
    break;
  case PropertyType::kDate:
    in_archive << value.type << value.value.d.milli_second;
    break;
  case PropertyType::kString:
    in_archive << value.type << value.value.s;
    break;
  default:
    in_archive << PropertyType::kEmpty;
    break;
  }

  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Any& value) {
  out_archive >> value.type;
  switch (value.type) {
  case PropertyType::kInt32:
    out_archive >> value.value.i;
    break;
  case PropertyType::kInt64:
    out_archive >> value.value.l;
    break;
  case PropertyType::kDate:
    out_archive >> value.value.d.milli_second;
    break;
  case PropertyType::kString:
    out_archive >> value.value.s;
    break;
  default:
    break;
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

}  // namespace gs
