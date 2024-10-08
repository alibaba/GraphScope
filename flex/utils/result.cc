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

#include "flex/utils/result.h"

namespace gs {
Status::Status() noexcept : error_code_(StatusCode::OK) {}

Status::Status(StatusCode error_code) noexcept : error_code_(error_code) {}

Status::Status(StatusCode error_code, std::string&& error_msg) noexcept
    : error_code_(error_code), error_msg_(std::move(error_msg)) {}

Status::Status(StatusCode error_code, const std::string& error_msg) noexcept
    : error_code_(error_code), error_msg_(error_msg) {}

std::string Status::error_message() const { return error_msg_; }

StatusCode Status::error_code() const { return error_code_; }

bool Status::ok() const { return error_code_ == StatusCode::OK; }

Status Status::OK() { return Status(StatusCode::OK); }

std::string Status::ToString() const {
  return "{\"code\": " + std::to_string(error_code_) + ", \"message\": \"" +
         error_msg_ + "\"}";
}

}  // namespace gs
