#include "flex/storages/mutable_csr/property/types.h"

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

inline void ParseIpAddr(const std::string_view& str, IpAddr& ip_addr) {
  ip_addr.from_str(str.data(), str.size());
}

inline void ParseString(const std::string_view& str, std::string_view& val) {
  val = str;
}

void ParseBrowser(const std::string_view& str, Browser& val) {
  if (str == "Internet Explorer") {
    val = Browser::kIE;
  } else if (str == "Opera") {
    val = Browser::kOpera;
  } else if (str == "Firefox") {
    val = Browser::kFirefox;
  } else if (str == "Chrome") {
    val = Browser::kChrome;
  } else if (str == "Safari") {
    val = Browser::kSafari;
  } else {
    LOG(FATAL) << "Unrecognized browser: " << str;
  }
}

inline void ParseGender(const std::string_view& str, Gender& val) {
  if (str == "male") {
    val = Gender::kMale;
  } else {
    val = Gender::kFemale;
  }
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
    } else if (item.type == PropertyType::kBrowser) {
      ParseBrowser(sv, item.value.b);
    } else if (item.type == PropertyType::kIpAddr) {
      ParseIpAddr(sv, item.value.ip);
    } else if (item.type == PropertyType::kGender) {
      ParseGender(sv, item.value.g);
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
    } else if (item.type == PropertyType::kBrowser) {
      ParseBrowser(sv, item.value.b);
    } else if (item.type == PropertyType::kIpAddr) {
      ParseIpAddr(sv, item.value.ip);
    } else if (item.type == PropertyType::kGender) {
      ParseGender(sv, item.value.g);
    }
    cur = ptr + 1;
  }
}

void ParseRecord(const char* line, int64_t& src, int64_t& dst,
                 std::vector<Any>& rec) {
  const char* cur = line;
  {
    const char* ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view sv(cur, ptr - cur);
    ParseInt64(sv, src);
    cur = ptr + 1;
  }
  {
    const char* ptr = cur;
    while (*ptr != '\0' && *ptr != '|') {
      ++ptr;
    }
    std::string_view sv(cur, ptr - cur);
    ParseInt64(sv, dst);
    cur = ptr + 1;
  }
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
    } else if (item.type == PropertyType::kBrowser) {
      ParseBrowser(sv, item.value.b);
    } else if (item.type == PropertyType::kIpAddr) {
      ParseIpAddr(sv, item.value.ip);
    } else if (item.type == PropertyType::kGender) {
      ParseGender(sv, item.value.g);
    }
    cur = ptr + 1;
  }
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
  case PropertyType::kBrowser:
    in_archive << value.type << value.value.b;
    break;
  case PropertyType::kIpAddr:
    in_archive << value.type << value.value.ip;
    break;
  case PropertyType::kGender:
    in_archive << value.type << value.value.g;
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
  case PropertyType::kBrowser:
    out_archive >> value.value.b;
    break;
  case PropertyType::kIpAddr:
    out_archive >> value.value.ip;
    break;
  case PropertyType::kGender:
    out_archive >> value.value.g;
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

inline static uint32_t str_x_to_number(const char* str, int len) {
  uint32_t ret = 0;
  for (int i = 0; i < len; ++i) {
    ret = ret * 10 + char_to_digit(str[i]);
  }
  return ret;
}

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

inline static void number_to_str_4(uint32_t n, char* str) {
  str[0] = (n / 1000u) + '0';
  n = n % 1000u;
  str[1] = (n / 100u) + '0';
  n = n % 100u;
  str[2] = (n / 10u) + '0';
  n = n % 10u;
  str[3] = n + '0';
}

inline static void number_to_str_3(uint32_t n, char* str) {
  str[0] = (n / 100u) + '0';
  n = n % 100u;
  str[1] = (n / 10u) + '0';
  n = n % 10u;
  str[2] = n + '0';
}

inline static void number_to_str_2(uint32_t n, char* str) {
  str[0] = (n / 10u) + '0';
  n = n % 10u;
  str[1] = n + '0';
}

Date::Date(int64_t x) : milli_second(x) {}
Date::Date(const char* str) { reset(str); }

int64_t get_date_milli_seconds(const std::string_view& str) {
  struct tm v;
  memset(&v, 0, sizeof(v));
  v.tm_year = str_4_to_number(str.data()) - 1900;
  v.tm_mon = str_2_to_number(&str[4]) - 1;
  v.tm_mday = str_2_to_number(&str[6]);
  int64_t milli_second = (mktime(&v));
  milli_second *= 1000l;
  milli_second += 8 * 60 * 60 * 1000l;
  return milli_second;
}

void Date::reset(const char* str) {
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
}

std::string Date::to_string() const { return std::to_string(milli_second); }



IpAddr::IpAddr(const std::string& str) { from_str(str.data(), str.size()); }

IpAddr::IpAddr(const std::string_view& str) { from_str(str.data(), str.size()); }

void IpAddr::from_str(const char* str, size_t size) {
  int loc[3];
  int cur = 0;
  for (size_t i = 0; i < size; ++i) {
    if (str[i] == '.') {
      loc[cur++] = i;
    }
  }
  a = str_x_to_number(str, loc[0]);
  b = str_x_to_number(str + loc[0] + 1, loc[1] - loc[0] - 1);
  c = str_x_to_number(str + loc[1] + 1, loc[2] - loc[1] - 1);
  d = str_x_to_number(str + loc[2] + 1, size - loc[2] - 1);
}

std::string IpAddr::to_string() const {
  return std::to_string(a) + "." + std::to_string(b) + "." + std::to_string(c) +
         "." + std::to_string(d);
}

void IpAddr::from_int(int value) {
  uint32_t casted = static_cast<uint32_t>(value);
  d = (casted & 0xFF);
  casted >>= 8;
  c = (casted & 0xFF);
  casted >>= 8;
  b = (casted & 0xFF);
  casted >>= 8;
  a = (casted & 0xFF);
}

int IpAddr::to_int() const {
  uint32_t ret = a;
  ret <<= 8;
  ret |= b;
  ret <<= 8;
  ret |= c;
  ret <<= 8;
  ret |= d;
  return static_cast<int>(ret);
}

grape::InArchive& operator<<(grape::InArchive& in_archive, const IpAddr& value) {
  in_archive << value.a << value.b << value.c << value.d;
  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, IpAddr& value) {
  out_archive >> value.a >> value.b >> value.c >> value.d;
  return out_archive;
}

std::string browser_to_string(Browser b) {
  if (b == Browser::kIE) {
    return "Internet Explorer";
  } else if (b == Browser::kFirefox) {
    return "Firefox";
  } else if (b == Browser::kOpera) {
    return "Opera";
  } else if (b == Browser::kChrome) {
    return "Chrome";
  } else if (b == Browser::kSafari) {
    return "Safari";
  } else {
    LOG(FATAL) << "Unrecognized browser enum";
    return "";
  }
}

Browser string_to_browser(const std::string_view& str) {
  if (str == "Internet Explorer") {
    return Browser::kIE;
  } else if (str == "Firefox") {
    return Browser::kFirefox;
  } else if (str == "Opera") {
    return Browser::kOpera;
  } else if (str == "Chrome") {
    return Browser::kChrome;
  } else if (str == "Safari") {
    return Browser::kSafari;
  } else {
    LOG(FATAL) << "Unrecognized browser string";
    return Browser::kIE;
  }
}

}  // namespace gs
