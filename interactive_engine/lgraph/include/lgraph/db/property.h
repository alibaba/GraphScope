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

#include "lgraph/db/error.h"
#include "lgraph/util/result.h"

namespace LGRAPH_NAMESPACE {
namespace db {

class Vertex;
class Edge;
class PropertyIterator;

class Property {
public:
  ~Property();

  // Move Only!
  // Avoid copy construction and assignment.
  Property(const Property &) = delete;
  Property &operator=(const Property &) = delete;
  Property(Property &&p) noexcept;
  Property &operator=(Property &&p) noexcept;

  bool Valid() const { return handle_ != nullptr; }

  PropertyId GetPropertyId();
  Result<int32_t, Error> GetAsInt32();
  Result<int64_t, Error> GetAsInt64();
  Result<float, Error> GetAsFloat();
  Result<double, Error> GetAsDouble();
  Result<StringSlice, Error> GetAsStr();

private:
  PropertyHandle handle_;

  // Hide constructors from users.
  Property();
  explicit Property(PropertyHandle handle);

  friend class Vertex;
  friend class Edge;
  friend class PropertyIterator;
};

class PropertyIterator {
public:
  ~PropertyIterator();

  // Move Only!
  // Avoid copy construction and assignment.
  PropertyIterator(const PropertyIterator &) = delete;
  PropertyIterator &operator=(const PropertyIterator &) = delete;
  PropertyIterator(PropertyIterator &&pi) noexcept;
  PropertyIterator &operator=(PropertyIterator &&pi) noexcept;

  bool Valid() const { return handle_ != nullptr; }

  Result<Property, Error> Next();

private:
  PropertyIterHandle handle_;

  // Hide constructors from users.
  PropertyIterator();
  explicit PropertyIterator(PropertyIterHandle handle);

  friend class Vertex;
  friend class Edge;
};

inline Property::Property() : handle_(nullptr) {}

inline Property::Property(PropertyHandle handle) : handle_(handle) {}

inline Property::Property(Property &&p) noexcept: Property() {
  *this = std::move(p);
}

inline Property &Property::operator=(Property &&p) noexcept {
  if (this != &p) {
    this->~Property();
    handle_ = p.handle_;
    p.handle_ = nullptr;
  }
  return *this;
}

inline PropertyIterator::PropertyIterator() : handle_(nullptr) {}

inline PropertyIterator::PropertyIterator(PropertyIterHandle handle) : handle_(handle) {}

inline PropertyIterator::PropertyIterator(PropertyIterator &&pi) noexcept: PropertyIterator() {
  *this = std::move(pi);
}

inline PropertyIterator &PropertyIterator::operator=(PropertyIterator &&pi) noexcept {
  if (this != &pi) {
    this->~PropertyIterator();
    handle_ = pi.handle_;
    pi.handle_ = nullptr;
  }
  return *this;
}

}
}
