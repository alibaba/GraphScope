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

#ifndef UTILS_RESULT_H_
#define UTILS_RESULT_H_

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "flex/utils/error_pb/interactive.pb.h"

#include "glog/logging.h"

namespace gs {

using StatusCode = gs::flex::interactive::Code;

class Status {
 public:
  Status() noexcept;
  Status(StatusCode error_code) noexcept;
  Status(StatusCode error_code, std::string&& error_msg) noexcept;
  Status(StatusCode error_code, const std::string& error_msg) noexcept;
  bool ok() const;
  std::string error_message() const;
  StatusCode error_code() const;

  static Status OK();

  std::string ToString() const;

 private:
  StatusCode error_code_;
  std::string error_msg_;
};

// Define a class with name Result<T>, which is a template class
// Stores the result of a function that may fail.
// If the function succeeds, the result contains the value returned by the
// function. If the function fails, the result contains the error message. The
// result is always valid and can be queried for success or failure. If the
// result is successful, the value can be obtained by calling the value()
// method. If the result fails, the error message can be obtained by calling the
// error() method. The result can be converted to bool, and the result is true
// if the result is successful. The result can be converted to bool, and the
// result is false if the result fails.
template <typename T>
class Result {
 public:
  using ValueType = T;
  Result() : status_(StatusCode::OK) {}
  Result(const ValueType& value) : status_(StatusCode::OK), value_(value) {}
  Result(ValueType&& value)
      : status_(StatusCode::OK), value_(std::move(value)) {}
  Result(const Status& status, ValueType&& value)
      : status_(status), value_(std::move(value)) {}

  Result(const Status& status) : status_(status) {}

  Result(const Status& status, const ValueType& value)
      : status_(status), value_(value) {}

  Result(StatusCode code, const std::string& error_msg, const ValueType& value)
      : status_(code, error_msg), value_(value) {}

  Result(StatusCode code, std::string&& error_msg, const ValueType& value)
      : status_(code, std::move(error_msg)), value_(value) {}

  bool ok() const noexcept { return status_.ok(); }

  const Status& status() const noexcept { return status_; }
  ValueType& value() noexcept { return value_; }

  // return rvalue
  ValueType&& move_value() noexcept { return std::move(value_); }

 private:
  Status status_;
  ValueType value_;
};

template <typename T>
struct is_gs_result_type : std::false_type {};

template <typename T>
struct is_gs_result_type<Result<T>> : std::true_type {};

template <typename T>
struct is_gs_status_type : std::false_type {};

template <>
struct is_gs_status_type<Status> : std::true_type {};

// define a macro, which checks the return status of a function, if ok, continue
// to execute, otherwise, return the status.
// the macro accept the calling code of a function, and the function name.
#define RETURN_IF_NOT_OK(expr) \
  do {                         \
    auto status = (expr);      \
    if (!status.ok()) {        \
      return status;           \
    }                          \
  } while (0)

// a Macro automatically assign the return value of a function, which returns
// result to a variable, and check the status of the result, if ok, continue to
// execute, otherwise, return the status. the macro accept the calling code of a
// function, the function name, and the variable name.
// reference:
// https://github.com/boostorg/leaf/blob/develop/include/boost/leaf/error.hpp
#define ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(var, expr)                      \
  {                                                                        \
    auto&& FLEX_TMP_VAR = expr;                                            \
    static_assert(                                                         \
        ::gs::is_gs_result_type<                                           \
            typename std::decay<decltype(FLEX_TMP_VAR)>::type>::value,     \
        "The expression must return a Result type");                       \
    if (!FLEX_TMP_VAR.ok()) {                                              \
      return FLEX_TMP_VAR;                                                 \
    }                                                                      \
    var = std::forward<decltype(FLEX_TMP_VAR)>(FLEX_TMP_VAR).move_value(); \
  }

#define ASSIGN_AND_RETURN_IF_STATUS_NOT_OK(var, expr)                      \
  {                                                                        \
    auto&& FLEX_TMP_VAR = expr;                                            \
    static_assert(                                                         \
        ::gs::is_gs_status_type<                                           \
            typename std::decay<decltype(FLEX_TMP_VAR)>::type>::value,     \
        "The expression must return a Status type");                       \
    if (!FLEX_TMP_VAR.ok()) {                                              \
      return FLEX_TMP_VAR;                                                 \
    }                                                                      \
    var = std::forward<decltype(FLEX_TMP_VAR)>(FLEX_TMP_VAR).move_value(); \
  }

// A Marco automatically use a auto variable to store the return value of a
// function, which returns result, and check the status of the result, if ok,
// continue to execute, otherwise, return the status. the macro accept the
// calling code of a function, the function name, and the variable name.
#define FLEX_AUTO(var, expr) ASSIGN_AND_RETURN_IF_NOT_OK(auto var, expr)

// Return boost::leaf::error object with error code and error message,

#define RETURN_FLEX_LEAF_ERROR(code, msg) \
  return ::boost::leaf::new_error(        \
      gs::Status(::gs::flex::interactive::Code::code, msg))

}  // namespace gs

namespace std {
inline std::string to_string(const gs::flex::interactive::Code& status) {
  // format the code into 0x-xxxx, where multiple zeros are prepend to the code
  std::stringstream ss;
  ss << "05-" << std::setw(4) << std::setfill('0')
     << static_cast<int32_t>(status);
  return ss.str();
}
}  // namespace std

#endif  // UTILS_RESULT_H_