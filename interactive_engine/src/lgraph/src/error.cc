/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "db/graph/error.h"
#include "store_ffi/store_ffi.h"

namespace DB_NAMESPACE {

Error::~Error() {
  if (handle_ != nullptr) {
    ffi::ReleaseErrorHandle(handle_);
  }
}

std::string Error::GetInfo() {
  auto str_slice = ffi::GetErrorInfo(handle_);
  return std::string{static_cast<const char*>(str_slice.data), str_slice.len};
}

}  // namespace DB_NAMESPACE