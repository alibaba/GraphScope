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

#pragma once

#include <algorithm>
#include <string>

#include "lgraph/common/namespace.h"
#include "lgraph/common/types.h"

namespace LGRAPH_NAMESPACE {
namespace db {

class Snapshot;
class Property;
class PropertyIterator;
class VertexIterator;
class EdgeIterator;

class Error {
public:
  ~Error();

  // Move Only!
  // Avoid copy construction and assignment.
  Error(const Error &) = delete;
  Error &operator=(const Error &) = delete;
  Error(Error &&e) noexcept;
  Error &operator=(Error &&e) noexcept;

  std::string GetInfo();

private:
  ErrorHandle handle_;

  // Hide constructors from users.
  Error();
  explicit Error(ErrorHandle handle);

  friend class Snapshot;
  friend class Property;
  friend class PropertyIterator;
  friend class VertexIterator;
  friend class EdgeIterator;
};

inline Error::Error() : handle_(nullptr) {}

inline Error::Error(ErrorHandle handle) : handle_(handle) {}

inline Error::Error(Error &&e) noexcept: Error() {
  *this = std::move(e);
}

inline Error &Error::operator=(Error &&e) noexcept {
  if (this != &e) {
    this->~Error();
    handle_ = e.handle_;
    e.handle_ = nullptr;
  }
  return *this;
}

}
}
