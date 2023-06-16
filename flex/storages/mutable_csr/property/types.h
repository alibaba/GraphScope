#ifndef GRAPHSCOPE_PROPERTY_TYPES_H_
#define GRAPHSCOPE_PROPERTY_TYPES_H_

#include <assert.h>

#include <charconv>
#include <string_view>

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/types.h"
#include "flex/utils/mmap_array.h"

namespace gs {

enum class PropertyType {
  kInt32,
  kDate,
  kString,
  kBrowser,
  kIpAddr,
  kGender,
  kEmpty,
  kInt64,
};

enum class Gender {
  kMale,
  kFemale,
};

int64_t get_date_milli_seconds(const std::string_view& str);

struct Date {
  Date() = default;
  ~Date() = default;
  Date(int64_t x);
  Date(const char* str);

  void reset(const char* str);
  std::string to_string() const;

  int64_t milli_second;

bool operator == (const Date& rhs) const {
    return milli_second == rhs.milli_second;
}
bool operator != (const Date& rhs) const {
    return milli_second != rhs.milli_second;
}
bool operator < (const Date& rhs) const {
    return milli_second < rhs.milli_second;
}
bool operator <= (const Date& rhs) const {
    return milli_second <= rhs.milli_second;
}
bool operator > (const Date& rhs) const {
    return !(*this <= rhs);
}
bool operator >= (const Date& rhs) const {
    return !(*this < rhs);
}
};

struct IpAddr {
  IpAddr() = default;
  ~IpAddr() = default;
  IpAddr(const std::string& str);
  IpAddr(const std::string_view& str);

  uint8_t a;
  uint8_t b;
  uint8_t c;
  uint8_t d;

  void from_str(const char* str, size_t size);

  std::string to_string() const;

  void from_int(int value);

  int to_int() const;
};

enum class Browser {
  kIE = 0,
  kFirefox = 1,
  kOpera = 2,
  kChrome = 3,
  kSafari = 4,
};

std::string browser_to_string(Browser b);
Browser string_to_browser(const std::string_view& str);

union AnyValue {
  AnyValue() {}
  ~AnyValue() {}

  int i;
  int64_t l;
  Date d;
  Browser b;
  IpAddr ip;
  std::string_view s;
  Gender g;
};

template <typename T>
struct TypeName {
  inline static const std::string name() {
    return typeid(T).name();
  }  // namespace gs
};


template <>
struct TypeName<uint64_t> {
  inline static const std::string name() { return "uint64"; };
};
template <>
struct TypeName<int32_t> {
  inline static const std::string name() { return "int32_t"; };
};

template <>
struct TypeName<int64_t> {
  inline static const std::string name() { return "int64_t"; };
};

template <>
struct TypeName<Gender> {
  inline static const std::string name() { return "Gender"; };
};

template <>
struct TypeName<std::string_view> {
  inline static const std::string name() { return "string_view"; };
};

template <>
struct TypeName<IpAddr> {
  inline static const std::string name() { return "IpAddr"; };
};
template <>
struct TypeName<Browser> {
  inline static const std::string name() { return "Browser"; };
};
template <>
struct TypeName<Date> {
  inline static const std::string name() { return "Date"; };
};
template <typename T>
struct AnyConverter;

struct Any {
  Any() : type(PropertyType::kEmpty) {}
  ~Any() {}

  int64_t get_long() const {
    assert(type == PropertyType::kInt64);
    return value.l;
  }

  void set_integer(int v) {
    type = PropertyType::kInt32;
    value.i = v;
  }

  void set_long(int64_t v) {
    type = PropertyType::kInt64;
    value.l = v;
  }

  void set_date(int64_t v) {
    type = PropertyType::kDate;
    value.d.milli_second = v;
  }
  void set_date(Date v) {
    type = PropertyType::kDate;
    value.d = v;
  }

  void set_string(std::string_view v) {
    type = PropertyType::kString;
    value.s = v;
  }

  void set_browser(const std::string& b) {
    if (b == "Internet Explorer") {
      value.b = Browser::kIE;
    } else if (b == "Opera") {
      value.b = Browser::kOpera;
    } else if (b == "Firefox") {
      value.b = Browser::kFirefox;
    } else if (b == "Chrome") {
      value.b = Browser::kChrome;
    } else if (b == "Safari") {
      value.b = Browser::kSafari;
    } else {
      LOG(FATAL) << "Unrecognized browser: " << b;
    }
  }

  void set_browser(Browser b) {
    type = PropertyType::kBrowser;
    value.b = b;
  }

  void set_gender(const std::string& str) {
    type = PropertyType::kGender;
    if (str == "male") {
      value.g = Gender::kMale;
    } else {
      value.g = Gender::kFemale;
    }
  }

  void set_gender(Gender g) {
    type = PropertyType::kGender;
    value.g = g;
  }

  void set_ip_addr(const std::string& addr) {
    type = PropertyType::kIpAddr;
    value.ip.from_str(addr.c_str(), addr.size());
  }
  void set_ip_addr(IpAddr ip) {
    type = PropertyType::kIpAddr;
    value.ip = ip;
  }

  std::string to_string() const {
    if (type == PropertyType::kInt32) {
      return std::to_string(value.i);
    } else if (type == PropertyType::kInt64) {
      return std::to_string(value.l);
    } else if (type == PropertyType::kString) {
      return std::string(value.s.data(), value.s.size());
      //      return value.s.to_string();
    } else if (type == PropertyType::kDate) {
      return value.d.to_string();
    } else if (type == PropertyType::kBrowser) {
      return browser_to_string(value.b);
    } else if (type == PropertyType::kIpAddr) {
      return value.ip.to_string();
    } else if (type == PropertyType::kGender) {
      return (value.g == Gender::kMale ? "male" : "female");
    } else {
      LOG(FATAL) << "Unexpected property type: " << static_cast<int>(type);
      return "";
    }
  }

  std::string AsString() const {
    assert(type == PropertyType::kString);
    return std::string(value.s);
  }

  int64_t AsInt64() const {
    assert(type == PropertyType::kInt64);
    return value.l;
  }

  const std::string_view& AsStringView() const {
    assert(type == PropertyType::kString);
    return value.s;
  }

  const Date& AsDate() const {
    assert(type == PropertyType::kDate);
    return value.d;
  }

  template <typename T>
  static Any From(const T& value) {
    return AnyConverter<T>::to_any(value);
  }

  PropertyType type;
  AnyValue value;
};

template <typename T>
struct ConvertAny {
  static void to(const Any& value, T& out) {
    LOG(FATAL) << "Unexpected convert type...";
  }
};

template <>
struct ConvertAny<int> {
  static void to(const Any& value, int& out) {
    CHECK(value.type == PropertyType::kInt32);
    out = value.value.i;
  }
};

template <>
struct ConvertAny<int64_t> {
  static void to(const Any& value, int64_t& out) {
    CHECK(value.type == PropertyType::kInt64);
    out = value.value.l;
  }
};

template <>
struct ConvertAny<Date> {
  static void to(const Any& value, Date& out) {
    CHECK(value.type == PropertyType::kDate);
    out = value.value.d;
  }
};

template <>
struct ConvertAny<grape::EmptyType> {
  static void to(const Any& value, grape::EmptyType& out) {
    CHECK(value.type == PropertyType::kEmpty);
  }
};

template <>
struct ConvertAny<std::string> {
  static void to(const Any& value, std::string& out) {
    CHECK(value.type == PropertyType::kString);
    out = std::string(value.value.s);
  }
};

template <typename T>
struct AnyConverter {};

template <>
struct AnyConverter<int> {
  static constexpr PropertyType type = PropertyType::kInt32;

  static Any to_any(const int& value) {
    Any ret;
    ret.set_integer(value);
    return ret;
  }

  static AnyValue to_any_value(const int& value) {
    AnyValue ret;
    ret.i = value;
    return ret;
  }

  static const int& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kInt32);
    return value.value.i;
  }

  static const int& from_any_value(const AnyValue& value) { return value.i; }
};

template <>
struct AnyConverter<int64_t> {
  static constexpr PropertyType type = PropertyType::kInt64;

  static Any to_any(const int64_t& value) {
    Any ret;
    ret.set_long(value);
    return ret;
  }

  static AnyValue to_any_value(const int64_t& value) {
    AnyValue ret;
    ret.l = value;
    return ret;
  }

  static const int64_t& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kInt64);
    return value.value.l;
  }

  static const int64_t& from_any_value(const AnyValue& value) {
    return value.l;
  }
};

template <>
struct AnyConverter<Date> {
  static constexpr PropertyType type = PropertyType::kDate;

  static Any to_any(const Date& value) {
    Any ret;
    ret.set_date(value);
    return ret;
  }

  static AnyValue to_any_value(const Date& value) {
    AnyValue ret;
    ret.d = value;
    return ret;
  }

  static const Date& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kDate);
    return value.value.d;
  }

  static const Date& from_any_value(const AnyValue& value) { return value.d; }
};

template <>
struct AnyConverter<std::string_view> {
  static constexpr PropertyType type = PropertyType::kString;

  static Any to_any(const std::string_view& value) {
    Any ret;
    ret.set_string(value);
    return ret;
  }

  static AnyValue to_any_value(const std::string_view& value) {
    AnyValue ret;
    ret.s = value;
    return ret;
  }

  static const std::string_view& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kString);
    return value.value.s;
  }

  static const std::string_view& from_any_value(const AnyValue& value) {
    return value.s;
  }
};

template <>
struct AnyConverter<std::string> {
  static constexpr PropertyType type = PropertyType::kString;

  static Any to_any(const std::string& value) {
    Any ret;
    ret.set_string(value);
    return ret;
  }

  static AnyValue to_any_value(const std::string& value) {
    AnyValue ret;
    ret.s = value;
    return ret;
  }

  static std::string from_any(const Any& value) {
    CHECK(value.type == PropertyType::kString);
    return std::string(value.value.s);
  }

  static std::string from_any_value(const AnyValue& value) {
    return std::string(value.s);
  }
};

template <>
struct AnyConverter<Browser> {
  static constexpr PropertyType type = PropertyType::kBrowser;

  static Any to_any(const Browser& value) {
    Any ret;
    ret.set_browser(value);
    return ret;
  }

  static AnyValue to_any_value(const Browser& value) {
    AnyValue ret;
    ret.b = value;
    return ret;
  }

  static const Browser& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kBrowser);
    return value.value.b;
  }

  static const Browser& from_any_value(const AnyValue& value) {
    return value.b;
  }
};

template <>
struct AnyConverter<Gender> {
  static constexpr PropertyType type = PropertyType::kGender;

  static Any to_any(const Gender& value) {
    Any ret;
    ret.set_gender(value);
    return ret;
  }

  static AnyValue to_any_value(const Gender& value) {
    AnyValue ret;
    ret.g = value;
    return ret;
  }

  static const Gender& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kGender);
    return value.value.g;
  }

  static const Gender& from_any_value(const AnyValue& value) { return value.g; }
};

template <>
struct AnyConverter<IpAddr> {
  static constexpr PropertyType type = PropertyType::kIpAddr;

  static Any to_any(const IpAddr& value) {
    Any ret;
    ret.set_ip_addr(value);
    return ret;
  }

  static AnyValue to_any_value(const IpAddr& value) {
    AnyValue ret;
    ret.ip = value;
    return ret;
  }

  static const IpAddr& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kIpAddr);
    return value.value.ip;
  }

  static const IpAddr& from_any_value(const AnyValue& value) {
    return value.ip;
  }
};

template <>
struct AnyConverter<grape::EmptyType> {
  static constexpr PropertyType type = PropertyType::kEmpty;

  static Any to_any(const grape::EmptyType& value) {
    Any ret;
    return ret;
  }

  static AnyValue to_any_value(const grape::EmptyType& value) {
    AnyValue ret;
    return ret;
  }

  static grape::EmptyType from_any(const Any& value) {
    CHECK(value.type == PropertyType::kEmpty);
    return grape::EmptyType();
  }

  static grape::EmptyType from_any_value(const AnyValue& value) {
    return grape::EmptyType();
  }
};

void ParseRecord(const char* line, std::vector<Any>& rec);

void ParseRecord(const char* line, int64_t& id, std::vector<Any>& rec);

void ParseRecord(const char* line, int64_t& src, int64_t& dst,
                 std::vector<Any>& rec);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int64_t& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, Date& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  grape::EmptyType& prop);

grape::InArchive& operator<<(grape::InArchive& in_archive, const Any& value);
/*
#ifndef __APPLE__
inline grape::InArchive& operator<<(grape::InArchive& in_archive, const std::string_view& value){
  in_archive << value.size();
  in_archive.AddBytes(value.data(), value.size());
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive, std::string_view& value){
  size_t size;
  out_archive >> size;
  value = std::string_view(static_cast<const char*>(out_archive.GetBytes(size)), size);
  return out_archive;
}
#endif
*/
grape::OutArchive& operator>>(grape::OutArchive& out_archive, Any& value);

}  // namespace gs

namespace std {

inline ostream& operator<<(ostream& os, const gs::Gender& g) {
  os << ((g == gs::Gender::kMale) ? "male" : "female");
  return os;
}

inline ostream& operator<<(ostream& os, const gs::Date& dt) {
  os << dt.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const gs::IpAddr& ip) {
  os << ip.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const gs::Browser& b) {
  os << gs::browser_to_string(b);
  return os;
}

}  // namespace std

#endif  // GRAPHSCOPE_PROPERTY_TYPES_H_
