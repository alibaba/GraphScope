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

#include "glog/logging.h"

namespace gs {
enum class StatusCode {
  OK = 0,
  InValidArgument = 1,
  UnsupportedOperator = 2,
  AlreadyExists = 3,
  NotExists = 4,
  CodegenError = 5,
  UninitializedStatus = 6,
  InvalidSchema = 7,
  PermissionError = 8,
  IllegalOperation = 9,
  InternalError = 10,
  InvalidImportFile = 11,
  IOError = 12,
  NotFound = 13,
  QueryFailed = 14,
};

class Status {
 public:
  Status() noexcept;
  Status(StatusCode error_code) noexcept;
  Status(StatusCode error_code, std::string&& error_msg) noexcept;
  Status(StatusCode error_code, const std::string& error_msg) noexcept;
  bool ok() const;
  std::string error_message() const;

  static Status OK();

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
  Result() : status_(StatusCode::UninitializedStatus) {}
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

 private:
  Status status_;
  ValueType value_;
};

}  // namespace gs

#endif  // UTILS_RESULT_H_