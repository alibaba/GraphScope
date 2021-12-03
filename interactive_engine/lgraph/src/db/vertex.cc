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

#include "lgraph/db/vertex.h"
#include "lgraph/db/store_ffi/store_ffi.h"

namespace LGRAPH_NAMESPACE {
namespace db {

Vertex::~Vertex() {
  if (handle_ != nullptr) {
    ffi::ReleaseVertexHandle(handle_);
  }
}

VertexId Vertex::GetVertexId() {
  return ffi::GetVertexId(handle_);
}

LabelId Vertex::GetLabelId() {
  return ffi::GetVertexLabelId(handle_);
}

Property Vertex::GetPropertyBy(PropertyId prop_id) {
  return Property(ffi::GetVertexProperty(handle_, prop_id));
}

PropertyIterator Vertex::GetPropertyIterator() {
  return PropertyIterator(ffi::GetVertexPropertyIterator(handle_));
}

VertexIterator::~VertexIterator() {
  if (handle_ != nullptr) {
    ffi::ReleaseVertexIteratorHandle(handle_);
  }
}

Result<Vertex, Error> VertexIterator::Next() {
  ErrorHandle err_hdl = nullptr;
  VertexHandle vertex_hdl = ffi::VertexIteratorNext(handle_, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<Vertex, Error>(Ok(Vertex(vertex_hdl)));
  }
  return Result<Vertex, Error>(Err(Error(err_hdl)));
}

}
}
