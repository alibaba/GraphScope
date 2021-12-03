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

#include "lgraph/db/property.h"

namespace LGRAPH_NAMESPACE {
namespace db {

class Snapshot;
class VertexIterator;

class Vertex {
public:
  ~Vertex();

  // Move Only!
  // Avoid copy construction and assignment.
  Vertex(const Vertex &) = delete;
  Vertex &operator=(const Vertex &) = delete;
  Vertex(Vertex &&v) noexcept;
  Vertex &operator=(Vertex &&v) noexcept;

  bool Valid() const { return handle_ != nullptr; }

  VertexId GetVertexId();
  LabelId GetLabelId();
  Property GetPropertyBy(PropertyId prop_id);
  PropertyIterator GetPropertyIterator();

private:
  VertexHandle handle_;

  // Hide constructors from users.
  Vertex();
  explicit Vertex(VertexHandle handle);

  friend class Snapshot;
  friend class VertexIterator;
};

class VertexIterator {
public:
  ~VertexIterator();

  // Move Only!
  // Avoid copy construction and assignment.
  VertexIterator(const VertexIterator &) = delete;
  VertexIterator &operator=(const VertexIterator &) = delete;
  VertexIterator(VertexIterator &&vi) noexcept;
  VertexIterator &operator=(VertexIterator &&vi) noexcept;

  bool Valid() const { return handle_ != nullptr; }

  Result<Vertex, Error> Next();

private:
  VertexIterHandle handle_;

  // Hide constructors from users.
  VertexIterator();
  explicit VertexIterator(VertexIterHandle handle);

  friend class Snapshot;
};

inline Vertex::Vertex() : handle_(nullptr) {}

inline Vertex::Vertex(VertexHandle handle) : handle_(handle) {}

inline Vertex::Vertex(Vertex &&v) noexcept: Vertex() {
  *this = std::move(v);
}

inline Vertex &Vertex::operator=(Vertex &&v) noexcept {
  if (this != &v) {
    this->~Vertex();
    handle_ = v.handle_;
    v.handle_ = nullptr;
  }
  return *this;
}

inline VertexIterator::VertexIterator() : handle_(nullptr) {}

inline VertexIterator::VertexIterator(VertexIterHandle handle) : handle_(handle) {}

inline VertexIterator::VertexIterator(VertexIterator &&vi) noexcept: VertexIterator() {
  *this = std::move(vi);
}

inline VertexIterator &VertexIterator::operator=(VertexIterator &&vi) noexcept {
  if (this != &vi) {
    this->~VertexIterator();
    handle_ = vi.handle_;
    vi.handle_ = nullptr;
  }
  return *this;
}

}
}
