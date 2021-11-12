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
class EdgeIterator;

class Edge {
public:
  ~Edge();

  // Move Only!
  // Avoid copy construction and assignment.
  Edge(const Edge &) = delete;
  Edge &operator=(const Edge &) = delete;
  Edge(Edge &&e) noexcept;
  Edge &operator=(Edge &&e) noexcept;

  bool Valid() const { return handle_ != nullptr; }

  EdgeId GetEdgeId();
  EdgeRelation GetEdgeRelation();
  Property GetPropertyBy(PropertyId prop_id);
  PropertyIterator GetPropertyIterator();

private:
  EdgeHandle handle_;

  // Hide constructors from users.
  Edge();
  explicit Edge(EdgeHandle handle);

  friend class Snapshot;
  friend class EdgeIterator;
};

class EdgeIterator {
public:
  ~EdgeIterator();

  // Move Only!
  // Avoid copy construction and assignment.
  EdgeIterator(const EdgeIterator &) = delete;
  EdgeIterator &operator=(const EdgeIterator &) = delete;
  EdgeIterator(EdgeIterator &&ei) noexcept;
  EdgeIterator &operator=(EdgeIterator &&ei) noexcept;

  bool Valid() const { return handle_ != nullptr; }

  Result<Edge, Error> Next();

private:
  EdgeIterHandle handle_;

  // Hide constructors from users.
  EdgeIterator();
  explicit EdgeIterator(EdgeIterHandle handle);

  friend class Snapshot;
};

inline Edge::Edge() : handle_(nullptr) {}

inline Edge::Edge(EdgeHandle handle) : handle_(handle) {}

inline Edge::Edge(Edge &&e) noexcept: Edge() {
  *this = std::move(e);
}

inline Edge &Edge::operator=(Edge &&e) noexcept {
  if (this != &e) {
    this->~Edge();
    handle_ = e.handle_;
    e.handle_ = nullptr;
  }
  return *this;
}

inline EdgeIterator::EdgeIterator() : handle_(nullptr) {}

inline EdgeIterator::EdgeIterator(EdgeIterHandle handle) : handle_(handle) {}

inline EdgeIterator::EdgeIterator(EdgeIterator &&ei) noexcept: EdgeIterator() {
  *this = std::move(ei);
}

inline EdgeIterator &EdgeIterator::operator=(EdgeIterator &&ei) noexcept {
  if (this != &ei) {
    this->~EdgeIterator();
    handle_ = ei.handle_;
    ei.handle_ = nullptr;
  }
  return *this;
}

}
}
